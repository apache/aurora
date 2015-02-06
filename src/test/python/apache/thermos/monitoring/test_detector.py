import pytest

from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT
from apache.thermos.monitoring.detector import ChainedPathDetector, FixedPathDetector


def test_fixed_path_detector():
  # Default is TaskPath default
  fpd = FixedPathDetector()
  assert fpd.get_paths() == [DEFAULT_CHECKPOINT_ROOT]

  # Non-default
  root = '/var/lib/derp'
  fpd = FixedPathDetector(path=root)
  assert fpd.get_paths() == [root]


def test_fixed_path_detector_constructor():
  with pytest.raises(TypeError):
    FixedPathDetector(path=234)


def test_chained_path_detector():
  root1 = '/var/lib/derp1'
  root2 = '/var/lib/derp2'
  fpd1 = FixedPathDetector(path=root1)
  fpd2 = FixedPathDetector(path=root2)
  cpd = ChainedPathDetector(fpd1, fpd2)
  assert set(cpd.get_paths()) == set([root1, root2])


def test_chained_path_detector_constructor():
  with pytest.raises(TypeError):
    ChainedPathDetector(1, 2, 3)

  with pytest.raises(TypeError):
    ChainedPathDetector(FixedPathDetector(), 'hello')
