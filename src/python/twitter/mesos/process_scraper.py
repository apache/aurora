import re
from twitter.common import app
from twitter.common.process import ProcessProviderFactory

app.add_option("--mesos_task_id_regex", dest = "mesos_task_id_regex", default = "/var/run/nexus/(?P<task_id>[^/]*)",
               help = "the regular expression of getting the mesos task id from path. " +
                      "The group name in regex has to be task_id. For example:/var/run/nexus/(?P<task_id>[^/]*)")
app.add_option("--ignore_user_name", dest = "ignore_user_name", default = "root",
              help = "Ignore the username, split by comma")

def main(args, opts):
  task_info = []
  ps = ProcessProviderFactory.get()
  ps.collect_all()
  p = re.compile(opts.mesos_task_id_regex)
  for pid in ps.pids():
    handle = ps.get_handle(pid)
    if handle.user() in opts.ignore_user_name.split(","):
      continue

    path = handle.cwd()
    if not path:
      continue

    matches = p.search(path)
    if not matches:
      continue

    task_id = matches.group('task_id')
    if not task_id:
      continue

    task_info.append((pid, task_id))

  for pid, task_id in task_info:
    print "%d %s" % (pid, task_id)


app.main()