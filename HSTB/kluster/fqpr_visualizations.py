import numpy as np

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, FFMpegWriter
import matplotlib.cm as cm
from matplotlib.colors import LinearSegmentedColormap
from mpl_toolkits.mplot3d import Axes3D  # need this, is used in backend


class FqprVisualizations:
    """
    Visualizations in Matplotlib built on top of FQPR class.  Includes animations of beam vectors and vessel
    orientation.

    Processed fqpr_generation.Fqpr instance is passed in as argument
    """

    def __init__(self, fqpr):
        """

        Parameters
        ----------
        fqpr
            Fqpr instance to visualize
        """

        self.fqpr = fqpr

        self.orientation_sector = None
        self.orientation_quiver = None
        self.orientation_figure = None
        self.orientation_objects = None
        self.orientation_anim = None

        self.bpv_quiver = None
        self.bpv_dat = None
        self.bpv_datsec = None
        self.bpv_figure = None
        self.bpv_objects = None
        self.bpv_anim = None

    def _parse_plot_mode(self, mode: str):
        """
        Used for the soundings plot, parse the mode option and return the variable names to use in the plot, checking
        to see if they are valid for the dataset (self.fqpr)

        Parameters
        ----------
        mode
            One of 'svcorr' and 'georef', which variable you want to visualize

        Returns
        -------
        xvar: string, variable name for the x dimension
        yvar: string, variable name for the y dimension
        zvar: string, variable name for the z dimension
        """

        if mode == 'svcorr':
            xvar = 'alongtrack'
            yvar = 'acrosstrack'
            zvar = 'depthoffset'
        elif mode == 'georef':
            xvar = 'x'
            yvar = 'y'
            zvar = 'z'
        else:
            raise ValueError('Unrecognized mode, must be either "svcorr" or "georef"')

        modechks = [[v in sec] for v in [xvar, yvar, zvar] for sec in self.fqpr.multibeam.raw_ping]
        if not np.any(modechks):
            raise ValueError('{}: Unable to find one or more variables in the raw_ping records'.format(mode))
        return xvar, yvar, zvar

    def soundings_plot_3d(self, mode: str = 'svcorr', sec_idx: int = None, tme: float = None):
        """
        Plots a 3d representation of the alongtrack/acrosstrack/depth values generated by sv correct.  If sector is
        provided, isolates that sector.  If a time is provided, isolates that time.

        Parameters
        ----------
        mode
            str, either 'svcorr' to plot the svcorrected offsets, or 'georef' to plot the georeferenced soundings
        sec_idx
            int, optional if you wish to only plot that sector
        tme
            float, optional if you wish to only plot for that time

        Returns
        -------
        plt.Axes
            matplotlib axes object for plot
        """

        xvar, yvar, zvar = self._parse_plot_mode(mode)

        miny = self.fqpr.calc_min_var(yvar)
        maxy = self.fqpr.calc_max_var(yvar)

        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        if sec_idx is None:
            sec_idx = self.fqpr.return_sector_ids()
        for sec in sec_idx:
            if tme is not None:
                if tme not in self.fqpr.multibeam.raw_ping[sec].time:
                    x = self.fqpr.multibeam.select_array_from_rangeangle(xvar, sec).sel(time=tme).stack(stck=('time', 'beam')).values
                    y = self.fqpr.multibeam.select_array_from_rangeangle(yvar, sec).sel(time=tme).stack(stck=('time', 'beam')).values
                    z = self.fqpr.multibeam.select_array_from_rangeangle(zvar, sec).sel(time=tme).stack(stck=('time', 'beam')).values
                else:
                    print('Unable to find time {} in sector {}'.format(tme, sec))
                    continue
            else:
                x = self.fqpr.multibeam.select_array_from_rangeangle(xvar, sec).stack(stck=('time', 'beam')).values
                y = self.fqpr.multibeam.select_array_from_rangeangle(yvar, sec).stack(stck=('time', 'beam')).values
                z = self.fqpr.multibeam.select_array_from_rangeangle(zvar, sec).stack(stck=('time', 'beam')).values

            x = x[~np.isnan(x)]
            y = y[~np.isnan(y)]
            z = z[~np.isnan(z)]
            ax.scatter(x, y, -z)
        ax.set_xlim(miny, maxy)
        return ax

    def soundings_plot_2d(self, mode: str = 'svcorr', color_by: str = 'depth', sec_idx: int = None, tme: float = None):
        """
        Plots a 2d representation of the acrosstrack/depth values generated by sv correct.  If sector is
        provided, isolates that sector.  If a time is provided, isolates that time.

        Parameters
        ----------
        mode
            str, either 'svcorr' to plot the svcorrected offsets, or 'georef' to plot the georeferenced soundings
        color_by
            str, either 'depth' or 'sector'
        sec_idx
            int, optional if you wish to only plot that sector
        tme
            float, optional if you wish to only plot for that time

        Returns
        -------
        plt.Figure
            matplotlib.pyplot.figure instance
        """

        xvar, yvar, zvar = self._parse_plot_mode(mode)

        minz = self.fqpr.calc_min_var(zvar)
        maxz = self.fqpr.calc_max_var(zvar)
        miny = self.fqpr.calc_min_var(yvar)
        maxy = self.fqpr.calc_max_var(yvar)
        minx = self.fqpr.calc_min_var(xvar)
        maxx = self.fqpr.calc_max_var(xvar)

        fig = plt.figure()
        if sec_idx is None:
            sec_idx = self.fqpr.return_sector_ids()
        for cnt, sec in enumerate(sec_idx):
            if tme is not None:
                try:
                    times_this_sector = tme[np.isin(tme, self.fqpr.multibeam.raw_ping[cnt].time)]
                except TypeError:  # float is provided
                    tme = np.array(tme)
                    times_this_sector = tme[tme in self.fqpr.multibeam.raw_ping[cnt].time]
                if np.any(times_this_sector):
                    x = self.fqpr.multibeam.select_array_from_rangeangle(xvar, sec).sel(time=times_this_sector).stack(stck=('time', 'beam')).values
                    y = self.fqpr.multibeam.select_array_from_rangeangle(yvar, sec).sel(time=times_this_sector).stack(stck=('time', 'beam')).values
                    z = self.fqpr.multibeam.select_array_from_rangeangle(zvar, sec).sel(time=times_this_sector).stack(stck=('time', 'beam')).values
                else:
                    print('Unable to find time {} in sector {}'.format(tme, sec))
                    continue
            else:
                x = self.fqpr.multibeam.select_array_from_rangeangle(xvar, sec).stack(stck=('time', 'beam')).values
                y = self.fqpr.multibeam.select_array_from_rangeangle(yvar, sec).stack(stck=('time', 'beam')).values
                z = self.fqpr.multibeam.select_array_from_rangeangle(zvar, sec).stack(stck=('time', 'beam')).values

            x = x[~np.isnan(x)]
            y = y[~np.isnan(y)]

            if color_by == 'depth':
                z = z[~np.isnan(z)]
                plt.scatter(y, x, marker='+', c=z, cmap='coolwarm', s=5)
                plt.clim(minz, maxz)
            elif color_by == 'sector':
                plt.scatter(y, x, marker='+', s=5)
        plt.xlim(miny, maxy)
        plt.ylim(minx, maxx)
        if color_by != 'sector':
            plt.colorbar().set_label(zvar, rotation=270, labelpad=10)
        plt.title('{}: {}/{} colored by {}'.format(mode, xvar, yvar, color_by))
        return fig

    def _generate_orientation_vector(self, sec_idx: int, tme: float = None):
        """
        Generate tx/rx vector data for given time value, return with values to be used with matplotlib quiver

        Parameters
        ----------
        sec_idx
            int, optional if you wish to only plot that sector
        tme
            float, time at this specific interval

        Returns
        -------
        tuple
            x component of starting location of vectors
        tuple
            y component of starting location of vectors
        tuple
            z component of starting location of vectors
        tuple
            x direction component of vectors
        tuple
            y direction component of vectors
        tuple
            z direction component of vectors
        """

        if tme is not None:
            tx = self.fqpr.multibeam.raw_ping[sec_idx].tx.sel(time=tme).values
            rx = self.fqpr.multibeam.raw_ping[sec_idx].rx.sel(time=tme).values
        else:
            tx = self.fqpr.multibeam.raw_ping[sec_idx].tx.isel(time=0).values
            rx = self.fqpr.multibeam.raw_ping[sec_idx].rx.isel(time=0).values
        # rx = rx[~np.all(np.isnan(rx), axis=1)]  # dont include the nan vector entries
        rx = np.nanmean(rx, axis=0)
        origin = [0, 0, 0]
        x, y, z = zip(origin, origin)
        u, v, w = zip(tx, rx)
        return x, y, z, u, v, w

    def _update_orientation_vector(self, time: float):
        """
        Update method for visualize_orientation_vector, runs on each frame of the animation

        Parameters
        ----------
        time
            float, time at this specific interval
        """

        vecdata = self._generate_orientation_vector(self.orientation_sector, time)
        tx_x = round(vecdata[3][0], 3)
        tx_y = round(vecdata[4][0], 3)
        tx_z = round(vecdata[5][0], 3)
        rx_x = round(vecdata[3][1], 3)
        rx_y = round(vecdata[4][1], 3)
        rx_z = round(vecdata[5][1], 3)

        self.orientation_quiver.remove()
        self.orientation_quiver = self.orientation_figure.quiver(*vecdata, color=['blue', 'red'])
        self.orientation_objects['time'].set_text('Time: {:0.3f}'.format(time))
        self.orientation_objects['tx_vec'].set_text('TX Vector: x:{:0.3f}, y:{:0.3f}, z:{:0.3f}'.format(tx_x, tx_y, tx_z))
        self.orientation_objects['rx_vec'].set_text('RX Vector: x:{:0.3f}, y:{:0.3f}, z:{:0.3f}'.format(rx_x, rx_y, rx_z))

    def visualize_orientation_vector(self, sec_idx: int = None):
        """
        Use matplotlib funcanimation to build animated representation of the transmitter/receiver across time

        Receiver orientation is based on attitude at the average time of receive (receive time differs across beams)

        Parameters
        ----------
        sec_idx
            int, optional if you wish to only plot that sector
        """

        if sec_idx is None:
            sec_idx = 0

        self.orientation_objects = {}
        self.fqpr.multibeam.raw_ping[sec_idx]['tx'] = self.fqpr.multibeam.raw_ping[sec_idx]['tx'].compute()
        self.fqpr.multibeam.raw_ping[sec_idx]['rx'] = self.fqpr.multibeam.raw_ping[sec_idx]['rx'].compute()

        fig = plt.figure(figsize=(10, 8))
        self.orientation_figure = fig.add_subplot(111, projection='3d')
        self.orientation_figure.set_xlim(-1.2, 1.2)
        self.orientation_figure.set_ylim(-1.2, 1.2)
        self.orientation_figure.set_zlim(-1.2, 1.2)
        self.orientation_figure.set_xlabel('+ Forward')
        self.orientation_figure.set_ylabel('+ Starboard')
        self.orientation_figure.set_zlabel('+ Down')

        self.orientation_objects['time'] = self.orientation_figure.text2D(-0.1, 0.11, '')
        self.orientation_objects['tx_vec'] = self.orientation_figure.text2D(0, 0.11, '', color='blue')
        self.orientation_objects['rx_vec'] = self.orientation_figure.text2D(0, 0.10, '', color='red')

        tme_interval = (self.fqpr.multibeam.raw_ping[sec_idx].time.values[1] -
                        self.fqpr.multibeam.raw_ping[sec_idx].time.values[0]) * 1000
        print('Animating with frame interval of {}'.format(int(tme_interval)))

        self.orientation_sector = sec_idx
        self.orientation_quiver = self.orientation_figure.quiver(*self._generate_orientation_vector(sec_idx),
                                                                 color=['blue', 'red'])
        self.orientation_anim = FuncAnimation(fig, self._update_orientation_vector,
                                              frames=self.fqpr.multibeam.raw_ping[sec_idx].time.values,
                                              interval=tme_interval)

    def _generate_bpv_arrs(self, dat: list):
        """
        Generate traveltime/beampointingangle vectors to be used with matplotlib quiver

        Parameters
        ----------
        dat
            list, beampointingangle/twowaytraveltime reformed across sectors

        Returns
        -------
        tuple
            x component of starting location of vectors
        tuple
            y component of starting location of vectors
        tuple
            x direction component of vectors
        tuple
            y direction component of vectors
        """

        bpa = np.array(dat[0]).ravel()
        tt = np.array(dat[1]).ravel()

        valid_bpa = ~np.isnan(bpa)
        valid_tt = ~np.isnan(tt)
        valid_idx = np.logical_and(valid_bpa, valid_tt)
        bpa = bpa[valid_idx]
        tt = tt[valid_idx]

        maxbeams = bpa.shape[0]
        u = np.sin(bpa) * tt
        v = np.cos(bpa) * tt
        u = -u / np.max(u)  # negative here for beam pointing angle so the port angles (pos) are on the left side
        v = -v / np.max(v)  # negative here for travel time so the vectors point down in the graph

        x = np.zeros(maxbeams)
        y = np.zeros(maxbeams)
        return x, y, u, v

    def _update_bpv(self, idx: int):
        """
        Update method for visualize_beam_pointing_vectors, runs on each frame of the animation

        Parameters
        ----------
        idx
            int, ping counter index
        """
        angles = self.bpv_dat[0, idx, :]
        traveltime = self.bpv_dat[1, idx, :]
        valid_bpa = ~np.isnan(angles)
        valid_tt = ~np.isnan(traveltime)
        valid_idx = np.logical_and(valid_bpa, valid_tt)
        angles = angles[valid_idx]
        traveltime = traveltime[valid_idx]

        if self.bpv_quiver is not None:
            self.bpv_quiver.remove()
        if self.fqpr.multibeam.is_dual_head():
            nextangles = self.bpv_dat[0, idx + 1, :]
            nexttraveltime = self.bpv_dat[1, idx + 1, :]
            nextvalid_bpa = ~np.isnan(nextangles)
            nextvalid_tt = ~np.isnan(nexttraveltime)
            nextvalid_idx = np.logical_and(nextvalid_bpa, nextvalid_tt)
            nextangles = nextangles[nextvalid_idx]
            nexttraveltime = nexttraveltime[nextvalid_idx]

            pouterang = [str(round(np.rad2deg(angles[0]), 3)), str(round(np.rad2deg(nextangles[0]), 3))]
            poutertt = [str(round(traveltime[0], 3)), str(round(nexttraveltime[0], 3))]
            pinnerang = [str(round(np.rad2deg(angles[-1]), 3)), str(round(np.rad2deg(nextangles[-1]), 3))]
            pinnertt = [str(round(traveltime[-1], 3)), str(round(nexttraveltime[-1], 3))]
            idx = [idx, idx + 1]
        else:
            pouterang = str(round(np.rad2deg(angles[0]), 3))
            poutertt = str(round(traveltime[0], 3))
            pinnerang = str(round(np.rad2deg(angles[-1]), 3))
            pinnertt = str(round(traveltime[-1], 3))

        self.bpv_quiver = self.bpv_figure.quiver(*self._generate_bpv_arrs(self.bpv_dat[:, idx, :]),
                                                 color=self._generate_bpv_colors(self.bpv_datsec[0, idx, :].ravel()),
                                                 units='xy', scale=1)
        self.bpv_objects['Time'].set_text('Ping: {}'.format(idx))

        self.bpv_objects['Port_outer_angle'].set_text('Port outermost angle: {}°'.format(pouterang))
        self.bpv_objects['Port_outer_traveltime'].set_text('Port outermost traveltime: {}s'.format(poutertt))
        self.bpv_objects['Starboard_outer_angle'].set_text('Starboard outermost angle: {}°'.format(pinnerang))
        self.bpv_objects['Starboard_outer_traveltime'].set_text('Starboard outermost traveltime: {}s'.format(pinnertt))

    def _generate_bpv_colors(self, datsec: np.array):
        """
        Return colormap for beams identifying unique sectors as different colors

        Parameters
        ----------
        datsec
            array of sector identifiers associated with each beam

        Returns
        -------
        LinearSegmentedColormap
            matplotlib colormap for that ping, colored by sector
        """

        unique_sectors = np.unique(datsec)
        newsec = datsec
        for u in unique_sectors:
            # replace sector identifiers with an integer index
            newsec = np.where(newsec == u, np.where(unique_sectors == u)[0][0], newsec)
        colormap = cm.rainbow
        newsec = newsec.astype(np.int)
        if np.max(newsec) > 0:
            # scale for the max integer count of sectors
            return colormap(newsec / np.max(newsec))
        else:
            return colormap(newsec)

    def _determine_bpv_framerate(self, timestmps: np.array):
        """
        Given the timestamps provided to the animation, determine the frame rate.

        Some sonars have two pings, diff freq each ping.  Identify the max gap to build the framerate

        Parameters
        ----------
        timestmps
            numpy array of timestamps

        Returns
        -------
        float
            framerate for the plot
        """

        diff_tstmps = np.diff(timestmps[0:10])
        if len(diff_tstmps) > 1:
            multiplier = np.mean(diff_tstmps) * 1000
        else:
            multiplier = 1000
        maxdif = diff_tstmps[np.argmax(diff_tstmps)] * multiplier
        return maxdif

    def visualize_beam_pointing_vectors(self, corrected: bool = False):
        """
        Use matplotlib funcanimation to build animated representation of the beampointingvectors/traveltimes across
        time

        if corrected is True uses the 'corr_pointing_angle' variable that is corrected for mounting angles/attitude,
        otherwise plots the raw 'beampointingangle' variable that is uncorrected.

        Parameters
        ----------
        corrected
            if True uses the 'corr_pointing_angle', else raw beam pointing angle 'beampointingangle'
        """

        if not corrected and ('beampointingangle' not in self.fqpr.multibeam.raw_ping[0]):
            raise ValueError('Unable to plot the raw beampointingangle, not found in source data')
        elif corrected and ('corr_pointing_angle' not in self.fqpr.multibeam.raw_ping[0]):
            raise ValueError('Unable to plot the corrected corr_pointing_angle, not found in source data')

        self.bpv_objects = {}
        unique_times = self.fqpr.return_unique_times_across_sectors()
        line_bounds = list(self.fqpr.multibeam.raw_ping[0].multibeam_files.values())[0]
        msk = np.logical_and(line_bounds[0] < unique_times, unique_times < line_bounds[1])
        unique_times = unique_times[msk]

        fig = plt.figure(figsize=(10, 8))
        self.bpv_figure = fig.add_subplot(1, 1, 1)

        self.bpv_figure.set_xlim(-1.5, 1.5)
        self.bpv_figure.set_ylim(-1.5, 0.5)
        self.bpv_figure.set_xlabel('Acrosstrack (scaled)')
        self.bpv_figure.set_ylabel('Travel Time (scaled)')
        self.bpv_figure.set_axis_off()

        self.bpv_objects['Time'] = self.bpv_figure.text(-1.4, 0.45, '')
        self.bpv_objects['Port_outer_angle'] = self.bpv_figure.text(-1.4, 0.40, '')
        self.bpv_objects['Port_outer_traveltime'] = self.bpv_figure.text(-1.4, 0.35, '')
        self.bpv_objects['Starboard_outer_angle'] = self.bpv_figure.text(0.35, 0.40, '')
        self.bpv_objects['Starboard_outer_traveltime'] = self.bpv_figure.text(0.35, 0.35, '')

        if not corrected:
            self.bpv_dat, self.bpv_datsec, tms = self.fqpr.reform_2d_vars_across_sectors_at_time(['beampointingangle',
                                                                                                  'traveltime'],
                                                                                                 unique_times)
            self.bpv_dat[0, :, :] = np.deg2rad(self.bpv_dat[0, :, :])
        else:
            self.bpv_dat, self.bpv_datsec, tms = self.fqpr.reform_2d_vars_across_sectors_at_time(['corr_pointing_angle',
                                                                                                  'traveltime'],
                                                                                                 unique_times)

        if self.fqpr.multibeam.is_dual_head():
            frames = [int(i * 2) for i in range(int(self.bpv_dat.shape[1]/2))]
            interval = 60 * 2
        else:
            frames = [i for i in range(int(self.bpv_dat.shape[1]))]
            interval = 60
        self.bpv_anim = FuncAnimation(fig, self._update_bpv, frames=frames, interval=interval)


def save_animation_mpeg(anim_instance: FuncAnimation, output_pth: str):
    """
    Save a Matplotlib FuncAnimation object to Mpeg

    Parameters
    ----------
    anim_instance
        Matplotlib FuncAnimation object
    output_pth
        str, path to where you want the mpeg to be generated
    """

    ffwriter = FFMpegWriter()
    anim_instance.save(output_pth, writer=ffwriter)
