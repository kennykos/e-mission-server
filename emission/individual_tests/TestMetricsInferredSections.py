from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import arrow
import os

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.tests.common as etc

import emission.analysis.intake.cleaning.filter_accuracy as eaicf

import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.storage.decorations.local_date_queries as esdldq

from emission.net.api import metrics

class TestMetricsInferredSections(unittest.TestCase):
    def setUp(self):
        self.seed_mode_path = etc.copy_dummy_seed_for_inference()
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-21")
        self.testUUID1 = self.testUUID
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-27")
        etc.runIntakePipeline(self.testUUID1)
        etc.runIntakePipeline(self.testUUID)
        logging.info(
            "After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        self.aug_start_ts = 1438387200
        self.aug_end_ts = 1441065600
        self.day_start_dt = esdldq.get_local_date(self.aug_start_ts, "America/Los_Angeles")
        self.day_end_dt = esdldq.get_local_date(self.aug_end_ts, "America/Los_Angeles")

    def tearDown(self):
        self.clearRelatedDb()
        os.remove(self.seed_mode_path)

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID1})

    def testCountNoEntries(self):
        # Ensure that we don't crash if we don't find any entries
        # Should return empty array instead
        # Unlike in https://amplab.cs.berkeley.edu/jenkins/job/e-mission-server-prb/591/
        met_result_ld = metrics.summarize_by_local_date(self.testUUID,
                                                     ecwl.LocalDate({'year': 2000}),
                                                     ecwl.LocalDate({'year': 2001}),
                                                     'MONTHLY', ['count'], True)
        self.assertEqual(list(met_result_ld.keys()), ['aggregate_metrics', 'user_metrics'])
        self.assertEqual(met_result_ld['aggregate_metrics'][0], [])
        self.assertEqual(met_result_ld['user_metrics'][0], [])

        met_result_ts = metrics.summarize_by_timestamp(self.testUUID,
                                                       arrow.get(2000,1,1).timestamp,
                                                       arrow.get(2001,1,1).timestamp,
                                                        'm', ['count'], True)
        self.assertEqual(list(met_result_ts.keys()), ['aggregate_metrics', 'user_metrics'])
        self.assertEqual(met_result_ts['aggregate_metrics'][0], [])
        self.assertEqual(met_result_ts['user_metrics'][0], [])

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
