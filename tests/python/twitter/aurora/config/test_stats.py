from twitter.common.contextutil import temporary_file
from twitter.aurora.config import AuroraConfig
import pprint
import unittest
import sys

class TestSdPackerClient(unittest.TestCase):
  def test_simple_stats_usage(self):
    STATS_SIMPLE = """
stage_foobartender = Process(
  name = 'stage',
  cmdline = '{{packer[{{role}}][foobartender][latest].copy_command}}')

foobartender_p = Process(
  name = 'foobartender',
  cmdline = './app.py {{thermos.ports[http]}} {{thermos.ports[admin]}} {{mesos.instance}}'
)

foobartender_t = Task(
  resources = Resources(cpu = 1, ram = 1024 * MB, disk = 250 * MB),
  processes = [stage_foobartender, foobartender_p,
                Stats(library = "metrics", port = 'http') ],
  constraints = order(stage_foobartender, foobartender_p)
)

foobartender_job = Job(
  role = 'observe',
  environment = 'prod',
  name = 'foobartender',
  cluster = 'smf1',
  instances = '1',
  task = foobartender_t,
  announce = Announcer()
)

jobs = [foobartender_job]"""
    with temporary_file() as fp:
      fp.write(STATS_SIMPLE)
      fp.flush()
      simple_config = AuroraConfig.load(fp.name)
      assert simple_config.name() == 'foobartender'
      procs = simple_config.task(0).processes().get()
      assert len(procs) == 3
      stats_p_all = [proc for proc in procs if proc['name'] == 'stats']
      assert len(stats_p_all) == 1
      stats_p = stats_p_all[0]
      assert stats_p['daemon']
      assert stats_p['ephemeral']
      cmdline = stats_p['cmdline'].encode('utf-8')
      assert "-pulllibrary=metrics" in cmdline # by default
      assert "[absorber][live]" in cmdline # by default

  def test_bad_stats_library(self):
    STATS_BAD_LIBRARY = """
stage_foobartender = Process(
  name = 'stage',
  cmdline = '{{packer[{{role}}][foobartender][latest].copy_command}}')

foobartender_p = Process(
  name = 'foobartender',
  cmdline = './app.py {{thermos.ports[http]}} {{thermos.ports[admin]}} {{mesos.instance}}'
)

foobartender_t = Task(
  processes =  [stage_foobartender, foobartender_p, 
                Stats(library = "<<<<<<<<<< I DONT EXIST >>>>>>>>>>", port = 'http') ],
  constraints = order(stage_foobartender, foobartender_p)
)"""
    with temporary_file() as fp:
      fp.write(STATS_BAD_LIBRARY)
      fp.flush()
      self.assertRaises(AssertionError, AuroraConfig.load, fp.name)
      