import json
import unittest

from HSTB.kluster.modules.svcorrect import run_ray_trace_v2
from xarray import load_dataset
import xarray as xr

import numpy as np
from HSTB.tests.modules.module_test_arrays import expected_beam_azimuth, expected_corrected_beam_angles, \
    expected_alongtrack, expected_acrosstrack, expected_depth


class TestSvCorrect(unittest.TestCase):

    def test_svcorrect_module(self):
        dset = load_dataset('009_20170523_12119_FA2806.all') #load_dataset(RealFqpr())
        multibeam = dset.raw_ping[0].isel(time=0).expand_dims('time')
        cast = json.loads(multibeam.profile_1495599960)
        beam_azimuth = xr.DataArray(data=expected_beam_azimuth, dims=['time', 'beam'],
                                    coords={'time': multibeam.time.values,
                                            'beam': multibeam.beam.values})
        beam_angle = xr.DataArray(data=expected_corrected_beam_angles, dims=['time', 'beam'],
                                  coords={'time': multibeam.time.values,
                                          'beam': multibeam.beam.values})
        traveltime = multibeam.traveltime
        surface_ss = multibeam.soundspeed

        installation_params_time = list(dset.xyzrph['tx_r'].keys())[0]
        waterline = float(dset.xyzrph['waterline'][installation_params_time])
        additional_offsets = np.array([[0], [0], [0]])

        alongtrack, acrosstrack, depth = run_ray_trace_v2(cast, beam_azimuth, beam_angle, traveltime, surface_ss,
                                                          waterline,
                                                          additional_offsets)
        assert np.array_equal(alongtrack, expected_alongtrack)
        assert np.array_equal(acrosstrack, expected_acrosstrack)
        assert np.array_equal(depth, expected_depth)
