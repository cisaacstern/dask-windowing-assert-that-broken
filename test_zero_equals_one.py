import apache_beam as beam
import pytest
from apache_beam.runners.dask.dask_runner import DaskRunner
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that, equal_to

runners = ["DirectRunner", DaskRunner()]
runner_ids = ["DirectRunner", "DaskRunner"]


@pytest.mark.xfail(strict=True)  # These tests **SHOULD FAIL** because 0 != 1
@pytest.mark.parametrize("runner", runners, ids=runner_ids)
def test_zero_equals_one(runner):
    with test_pipeline.TestPipeline(runner=runner) as p:
        pcoll = p | beam.Create([0])
        assert_that(pcoll, equal_to([1]))
