import numpy as np
from copy import deepcopy
from time import perf_counter

from bathygrid.convenience import create_grid
from HSTB.kluster.fqpr_convenience import reprocess_sounding_selection
from HSTB.kluster import kluster_variables
from HSTB.kluster.fqpr_helpers import seconds_to_formatted_string


class PatchTest:
    """
    Patch test utility based on "Computation of Calibration Parameters for Multibeam Echo Sounders Using the
    Least Squares Method", by Jan Terje Bjorke.  Compute new offsets/angles for the data provided using this automated
    least squares adjustment.  Data should be provided only for a pair of lines, lines that are reciprocal and overlap as
    much as possible.  We expect the input to be a FQPR subset generated by Kluster.  We take the non-rejected points,
    build a Bathygrid gridded surface, construct the required matrices and store the result for each line (self.lstsq_result).
    The result should be of shape (6,2), with six patch test parameters available per line.

    The six parameters are roll, pitch, heading, x translation, y translation and horizontal scale factor.
    """

    def __init__(self, fqpr, azimuth: float, sonar_head_index: int = None):
        """
        Parameters
        ----------
        fqpr
            'Fully processed ping record', Kluster processed FQPR instance subset to just the two lines of interest
        azimuth
            azimuth of one of the lines that we will use to rotate all points
        sonar_head_index
            only used with dual head systems, 0 = port head, 1 = starboard head
        """

        self.fqpr = fqpr
        self.azimuth = azimuth
        if self.fqpr.multibeam.is_dual_head():
            if sonar_head_index is None:
                raise ValueError('PatchTest: Sonar head index must be provided if the sonar is a dual head system')
            self.sonar_head_index = sonar_head_index
            if sonar_head_index == 0:
                self.fqpr.multibeam.raw_ping[1] = None
                self.xyzrph_key = 'port_'
            elif sonar_head_index == 1:
                self.fqpr.multibeam.raw_ping[0] = None
                self.xyzrph_key = 'stbd_'
            else:
                raise ValueError('PatchTest: Sonar head index must be either 0 or 1 for selecting the head to use')
        else:
            self.xyzrph_key = ''
            self.sonar_head_index = 0

        self.xyzrph_timestamp = None  # the timestamp in utc seconds for the installation parameters record we use in the patch test
        self.initial_parameters = None  # the offsets/angles/uncertainty from the installation parameters record as it is initially
        self.current_parameters = None  # the offsets/angles/uncertainty according to the last adjustment made here
        self.last_adjustment = None  # the last delta for each patch test parameter
        self._convert_parameters()

        self.multibeam_files = self.fqpr.multibeam.raw_ping[0].multibeam_files  # lookup for time/position/azimuth for each line
        self.multibeam_indexes = None  # the integer index of the line to look up the corresponding points
        self.points = None  # numpy structured array of all the rotated points we use
        self.min_x = None  # the minimum easting of the points
        self.min_y = None  # the minimum northing of the points
        self.grid = None  # the grid corresponding to the current run, gridded Bathygrid object made with self.points
        self.a_matrix = None  # the A matrix for the least squares run
        self.b_matrix = None  # the B matrix for the least squares run
        self.lstsq_result = None  # the result of the least squares run, contains the patch test parameter delta for each line for each parameter

        self.updated_parameters = []
        self.reliability_factors = []

    def run_patch(self):
        """
        Run the patch test procedure, saving the adjustments to the result attribute.
        """
        print('Initializing patch test for lines {}'.format(list(self.multibeam_files.keys())))
        starttime = perf_counter()
        self._build_initial_points()
        endtime = perf_counter()
        print('Initialization complete: {}'.format(seconds_to_formatted_string(int(endtime - starttime))))
        for i in range(3):
            print('****Patch run {} start****'.format(i + 1))
            starttime = perf_counter()
            self._generate_rotated_points()
            self._grid()
            self._build_patch_test_values()
            self._compute_least_squares()
            self._reprocess_points()
            endtime = perf_counter()
            print('****Patch run {} complete: {}****'.format(i + 1, seconds_to_formatted_string(int(endtime - starttime))))

            # break here for troubleshooting
            # break

    def display_results(self):
        """
        Print the current adjustment value derived in the last least squares operation
        """

        print('Patch test results, run {} times'.format(len(self.updated_parameters)))
        print('----------------------------')
        if self.fqpr is not None and self.lstsq_result is not None:
            print('Lines: {}\n'.format(list(self.fqpr.multibeam.raw_ping[0].multibeam_files.keys())))
            print('roll: {}, reliability: {}'.format(self.current_parameters['roll'], self.reliability_factors[-1][0]))
            print('pitch: {}, reliability: {}'.format(self.current_parameters['pitch'], self.reliability_factors[-1][1]))
            print('heading: {}, reliability: {}'.format(self.current_parameters['heading'], self.reliability_factors[-1][2]))
            print('x offset: {}, reliability: {}'.format(self.current_parameters['x_offset'], self.reliability_factors[-1][3]))
            print('y offset: {}, reliability: {}'.format(self.current_parameters['y_offset'], self.reliability_factors[-1][4]))
            print('horizontal scale factor: {}, reliability: {}'.format(self.current_parameters['hscale_factor'], self.reliability_factors[-1][5]))

    def _convert_parameters(self):
        """
        All the current offsets and angles are stored in the xyzrph dict attribute in the Fqpr object.  Here we log the
        current offsets/angles to the initial_parameters attribute and a copy of those same parameters to the current_parameters
        attribute.  The current_parameters will be updated on each iteration of reprocessing
        """

        initial_parameters = self.fqpr.multibeam.xyzrph
        self.xyzrph_timestamp = list(initial_parameters['roll_sensor_error'].keys())[0]
        self.initial_parameters = {'roll': float(initial_parameters['rx_' + self.xyzrph_key + 'r'][self.xyzrph_timestamp]),
                                   'roll_unc': float(initial_parameters['roll_sensor_error'][self.xyzrph_timestamp]),
                                   'pitch': float(initial_parameters['rx_' + self.xyzrph_key + 'p'][self.xyzrph_timestamp]),
                                   'pitch_unc': float(initial_parameters['pitch_sensor_error'][self.xyzrph_timestamp]),
                                   'heading': float(initial_parameters['rx_' + self.xyzrph_key + 'h'][self.xyzrph_timestamp]),
                                   'heading_unc': float(initial_parameters['heading_sensor_error'][self.xyzrph_timestamp]),
                                   'x_offset': float(initial_parameters['rx_' + self.xyzrph_key + 'x'][self.xyzrph_timestamp]),
                                   'x_unc': float(initial_parameters['x_offset_error'][self.xyzrph_timestamp]),
                                   'y_offset': float(initial_parameters['rx_' + self.xyzrph_key + 'y'][self.xyzrph_timestamp]),
                                   'y_unc': float(initial_parameters['y_offset_error'][self.xyzrph_timestamp]),
                                   'hscale_factor': 0.0,
                                   'hscale_unc': 0.01}
        self.current_parameters = deepcopy(self.initial_parameters)

    def _compute_reliability(self, roll, pitch, heading, x_offset, y_offset, hscale_factor):
        """
        The reliability factor is our way of assessing the ability of this test dataset to actually generate good parameters.
        It looks at the difference between the last adjustment and the current adjustment, relative to the current working
        parameters.  This will tell you if your last least squares result is relatively close to your current one, which
        means the process has settled on a good answer.

        A reliability factor near 1 means you have a good answer.  A reliability factor near 0 means the process is not
        settling on a good answer for the parameter.

        Parameters
        ----------
        roll
            roll adjustment value in degrees
        pitch
            pitch adjustment value in degrees
        heading
            heading adjustment value in degrees
        x_offset
            x offset adjustment value in meters
        y_offset
            y offset adjustment value in meters
        hscale_factor
            horizontal scale factor
        """

        if not self.updated_parameters:  # this is the first run
            self.reliability_factors.append([0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        else:
            newadjust = [roll, pitch, heading, x_offset, y_offset, hscale_factor]
            curvals = [self.current_parameters['roll'], self.current_parameters['pitch'], self.current_parameters['heading'],
                       self.current_parameters['x_offset'], self.current_parameters['y_offset'], self.current_parameters['hscale_factor']]
            rfactors = []
            for adjust, newadjust, curval in zip(self.last_adjustment, newadjust, curvals):
                if curval == 0:
                    rfactor = 0
                else:
                    rfactor = max(min(1 - np.abs((adjust - newadjust) / curval), 1), 0)
                    if np.isnan(rfactor):
                        rfactor = 0
                rfactors.append(rfactor)
            self.reliability_factors.append(rfactors)

    def _adjust_original_xyzrph(self, roll, pitch, heading, x_offset, y_offset, hscale_factor):
        """
        Add the provided values to the xyzrph dictionary that the Fqpr uses during reprocessing.  Also cache the new
        values in the current_parameters dictionary object.

        Parameters
        ----------
        roll
            roll adjustment value in degrees
        pitch
            pitch adjustment value in degrees
        heading
            heading adjustment value in degrees
        x_offset
            x offset adjustment value in meters
        y_offset
            y offset adjustment value in meters
        hscale_factor
            horizontal scale factor
        """

        print('adjusting by: roll={}, pitch={}, heading={}, x_translation={}, y_translation={}, horizontal_scale_factor={}'.format(roll, pitch, heading, x_offset, y_offset, hscale_factor))
        self._compute_reliability(roll, pitch, heading, x_offset, y_offset, hscale_factor)

        tstmp = list(self.fqpr.multibeam.xyzrph['roll_sensor_error'].keys())[0]
        self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'r'][tstmp] += roll
        self.current_parameters['roll'] += roll
        self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'p'][tstmp] += pitch
        self.current_parameters['pitch'] += pitch
        self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'h'][tstmp] += heading
        self.current_parameters['heading'] += heading

        print('WARNING - xoffset yoffset application are disabled for testing')
        # self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'x'][tstmp] += x_offset
        # self.current_parameters['x_offset'] += x_offset
        # self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'y'][tstmp] += y_offset
        # self.current_parameters['y_offset'] += y_offset

        self.current_parameters['hscale_factor'] += hscale_factor

        self.last_adjustment = [roll, pitch, heading, x_offset, y_offset, hscale_factor]
        # updating the current_parameters also triggers storing the values in the results lists
        self.updated_parameters.append([self.current_parameters['roll'], self.current_parameters['pitch'],
                                        self.current_parameters['heading'], self.current_parameters['x_offset'],
                                        self.current_parameters['y_offset'], self.current_parameters['hscale_factor']])
        print('reprocessing with: roll={}, pitch={}, heading={}, x_translation={}, y_translation={}'.format(self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'r'][tstmp],
                                                                                                            self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'p'][tstmp],
                                                                                                            self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'h'][tstmp],
                                                                                                            self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'x'][tstmp],
                                                                                                            self.fqpr.multibeam.xyzrph['rx_' + self.xyzrph_key + 'y'][tstmp]))

    def _build_initial_points(self):
        """
        The first run, we can pull the points from the currently loaded Fqpr object and store them in the points
        numpy structured array.  We keep a multibeam_indexes lookup so that we can know which points go with which line
        file.  This is important later in constructing the L1 matrix.
        """

        curr_point_index = 0
        finalx = None
        finaly = None
        finalz = None
        self.points = None
        self.multibeam_indexes = {}
        for mfilename in self.multibeam_files.keys():
            data = self.fqpr.subset_variables_by_line(['x', 'y', 'z'], mfilename, filter_by_detection=True)[mfilename]
            x, y, z = data.x.values, data.y.values, data.z.values
            self.multibeam_indexes[mfilename] = [curr_point_index, curr_point_index + x.size]
            curr_point_index = curr_point_index + x.size
            if finalx is None:
                finalx = x
                finaly = y
                finalz = z
            else:
                finalx = np.concatenate([finalx, x])
                finaly = np.concatenate([finaly, y])
                finalz = np.concatenate([finalz, z])
        if finalx.any():
            dtyp = [('x', np.float64), ('y', np.float64), ('z', np.float32)]
            self.points = np.empty(len(finalx), dtype=dtyp)
            self.points['x'] = finalx
            self.points['y'] = finaly
            self.points['z'] = finalz

    def _reprocess_points(self):
        """
        Add the latest adjustment values to the Fqpr xyzrph record and reprocess with those values.  We reprocess using
        the in-memory workflow in Kluster, which means the results of the reprocessing (new georeferenced values) are not
        saved to disk, they are kept in the intermediate_data lookup.  Pull out those values and clear the computed Kluster
        result.  The new points are kept in the points numpy structured array for the next least squares operation.
        """

        roll = np.round(float(np.mean(self.lstsq_result[0])), 4)
        pitch = np.round(float(np.mean(self.lstsq_result[1])), 4)
        heading = np.round(float(np.mean(self.lstsq_result[2])), 4)
        x_translation = np.round(float(np.mean(self.lstsq_result[3])), 4)
        y_translation = np.round(float(np.mean(self.lstsq_result[4])), 4)
        hscale_factor = np.round(float(np.mean(self.lstsq_result[4])), 5)
        self._adjust_original_xyzrph(roll, pitch, heading, x_translation, y_translation, hscale_factor)
        newfq, _ = reprocess_sounding_selection(self.fqpr, georeference=True, turn_off_dask=False)

        curr_point_index = 0
        finalx = None
        finaly = None
        finalz = None
        self.points = None
        cached_data = None
        ra = self.fqpr.multibeam.raw_ping[self.sonar_head_index]
        for sector in newfq.intermediate_dat:
            if 'georef' in newfq.intermediate_dat[sector]:
                for tstmp in newfq.intermediate_dat[sector]['georef']:
                    # there should only be one cached_data in intermediate_dat, no need to break here
                    if cached_data is not None:
                        raise ValueError('PatchTest: reprocessing failed, found multiple cached datasets, which should not happen with one sonar and one xyzrph entry')
                    cached_data = newfq.intermediate_dat[sector]['georef'][tstmp]
        if cached_data is None:
            raise ValueError('PatchTest: reprocessing failed, no cached data found')

        mfiles = self.fqpr.return_line_dict()
        good_soundings = ra.detectioninfo != kluster_variables.rejected_flag
        for linename in mfiles.keys():
            starttime, endtime = mfiles[linename][0], mfiles[linename][1]
            # valid_index would be the boolean mask for the line we are currently looking at
            valid_index = np.logical_and(ra.time >= float(starttime), ra.time <= float(endtime))
            valid_goodsoundings = good_soundings[valid_index, :]
            x = np.concatenate([c[0][0] for c in cached_data])[valid_index, :][valid_goodsoundings]
            y = np.concatenate([c[0][1] for c in cached_data])[valid_index, :][valid_goodsoundings]
            z = np.concatenate([c[0][2] for c in cached_data])[valid_index, :][valid_goodsoundings]
            self.multibeam_indexes[linename] = [curr_point_index, curr_point_index + x.size]
            curr_point_index = curr_point_index + x.size
            if finalx is None:
                finalx = np.ravel(x)
                finaly = np.ravel(y)
                finalz = np.ravel(z)
            else:
                finalx = np.concatenate([finalx, x])
                finaly = np.concatenate([finaly, y])
                finalz = np.concatenate([finalz, z])
        self.fqpr.intermediate_dat = {}
        if finalx.any():
            dtyp = [('x', np.float64), ('y', np.float64), ('z', np.float32)]
            self.points = np.empty(len(finalx), dtype=dtyp)
            self.points['x'] = finalx
            self.points['y'] = finaly
            self.points['z'] = finalz

    def _generate_rotated_points(self):
        """
        Convert the northings/eastings/depths to the model coordinate system.  This coordinate system is defined as:

        x = + Forward, y = + Starboard, z = + Down, Roll = + Port down, Pitch = + Bow down, Yaw = + Counterclockwise

        The test computes the following parameters:

        roll, pitch, heading, x_translation, y_translation, horizontal scale factor

        First, we pull the valid soundings from the Fqpr instance, using the filter_by_detection option to remove
        rejected soundings.  The points are returned for each line, which we then rotate according to one of the line
        azimuths to get an eastern orientation, where +X would be forward for the sonar.  Then, we flip the y values
        to get positive to starboard and normalize both northings and eastings to get xy values in the new model coordinate
        system.

        The result is stored in the self.points attribute as a new structured numpy array.
        """

        ang = self.azimuth - 90  # rotations are counter clockwise, we want it eventually facing east
        cos_az = np.cos(np.deg2rad(ang))
        sin_az = np.sin(np.deg2rad(ang))

        print('Rotating points by {} degrees...'.format(ang))
        if self.points is not None:
            # # normalize the y axis
            # self.points['y'] = self.points['y'] - self.points['y'].min()

            # # normalize the x axis
            # self.points['x'] = self.points['x'] - self.points['x'].min()

            # calculate center of rotation, use the origin of the points
            self.min_x = self.points['x'].min()
            self.min_y = self.points['y'].min()
            origin_x = self.points['x'] - self.min_x
            origin_y = self.points['y'] - self.min_y

            # rotate according to the provided line azimuth
            self.points['x'] = self.min_x + cos_az * origin_x - sin_az * origin_y
            self.points['y'] = self.min_y + sin_az * origin_x + cos_az * origin_y

            # flip the y axis to make it +x forward, +y starboard, +z down
            self.points['y'] = self.points['y'].max() - self.points['y']
        else:
            print('Found no valid points for {}'.format(list(self.multibeam_files.keys())))

    def _grid(self):
        """
        Compute an in memory bathygrid grid, single resolution with depth automatically determined by the depth of the
        tiles, using the bathygrid depth lookup table.  We add points by line so that we can use the line name to return
        the gridded depth values for each line later.
        """

        if self.points is not None and self.points.size > 0:
            print('Building in memory grid for {} soundings...'.format(self.points.size))
            grid_class = create_grid(grid_type='single_resolution')
            for linename in self.multibeam_indexes:
                idxs = self.multibeam_indexes[linename]
                grid_class.add_points(self.points[idxs[0]:idxs[1]], linename, [linename], progress_bar=False)
            grid_class.grid(progress_bar=False)
            self.grid = grid_class

    def _build_patch_test_values(self):
        """
        Build the a and b matrices for the least squares calculation.  The equation used is:

        Parameters = (A_transpose * p_one * A + p_two) ( x ) = (A_transpose * p_one * l_one)

        Which leaves us wth self.a_matrix = A_transpose * p_one * A + p_two and self.b_matrix = A_transpose * p_one * l_one
        """

        if self.grid is not None:
            print('Building patch test matrices...')
            line_layers = list(self.multibeam_indexes.keys())
            dpth, xslope, yslope, lineone, linetwo = self.grid.get_layers_by_name(['depth', 'x_slope', 'y_slope', line_layers[0], line_layers[1]])
            valid_index = np.logical_and(~np.isnan(lineone), ~np.isnan(linetwo))
            if valid_index.any():
                xval = np.arange(self.grid.min_x, self.grid.max_x, self.grid.resolutions[0])
                yval = np.arange(self.grid.min_y, self.grid.max_y, self.grid.resolutions[0])
                grid_rez = self.grid.resolutions[0]
                # compute the x and y node locations for each grid node in the grid
                x_node_locs, y_node_locs = np.meshgrid(xval + grid_rez / 2, yval + grid_rez / 2, copy=False)

                dpth_valid = dpth[valid_index]  # grid depth for all grid nodes where both lines overlap
                y_node_valid = y_node_locs[valid_index]  # y coordinate for all grid nodes where both lines overlap
                xslope_valid = xslope[valid_index]  # grid slope partial x for all grid nodes where both lines overlap
                yslope_valid = yslope[valid_index]  # grid slope partial y for all grid nodes where both lines overlap
                lineone_valid = lineone[valid_index]  # grid depth for line one for all grid nodes where both lines overlap
                linetwo_valid = linetwo[valid_index]  # grid slope partial x for all grid nodes where both lines overlap

                # A-matrix is in order of roll, pitch, heading, x_translation, y_translation, horizontal scale factor
                a_matrix = np.column_stack([yslope_valid * dpth_valid - y_node_valid,
                                            xslope_valid * dpth_valid,
                                            xslope_valid * y_node_valid,
                                            xslope_valid,
                                            yslope_valid,
                                            yslope_valid * y_node_valid])
                l_one_matrix = np.column_stack([lineone_valid, linetwo_valid])
                # p_one can contain 1/grid node uncertainty in the future, currently we leave it out
                # p_one_matrix = np.identity(self.a_matrix.shape[0])
                p_two_matrix = np.identity(6) * [1 / self.initial_parameters['roll_unc'] ** 2, 1 / self.initial_parameters['pitch_unc'] ** 2,
                                               1 / self.initial_parameters['heading_unc'] ** 2, 1 / self.initial_parameters['x_unc'],
                                               1 / self.initial_parameters['y_unc'] ** 2, 1 / self.initial_parameters['hscale_unc']]
                print('weighted by {}'.format([1 / self.initial_parameters['roll_unc'] ** 2, 1 / self.initial_parameters['pitch_unc'] ** 2,
                                               1 / self.initial_parameters['heading_unc'] ** 2, 1 / self.initial_parameters['x_unc'],
                                               1 / self.initial_parameters['y_unc'] ** 2, 1 / self.initial_parameters['hscale_unc']]))
                a_t = a_matrix.T
                self.a_matrix = np.dot(a_t, a_matrix) + p_two_matrix
                # self.a_matrix = np.dot(a_t, a_matrix)
                self.b_matrix = np.dot(a_t, l_one_matrix)
            else:
                print('No valid overlap found for lines: {}'.format(list(self.multibeam_files.keys())))

    def _compute_least_squares(self):
        if self.a_matrix is not None and self.b_matrix is not None:
            print('Computing least squares result')
            self.lstsq_result, residuals, rank, singular = np.linalg.lstsq(self.a_matrix, self.b_matrix, rcond=None)
