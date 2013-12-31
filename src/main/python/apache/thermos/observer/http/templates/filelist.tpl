<%doc>
 Template arguments:
   task_id
   chroot
   path
   dirs
   files
</%doc>


<%!
  import os

  from datetime import datetime
  import grp
  import os
  import pwd
  import stat
  import sys

  NOW = datetime.now()

  def format_mode(sres):
    mode = sres.st_mode

    root = (mode & 0700) >> 6
    group = (mode & 0070) >> 3
    user = (mode & 07)

    def stat_type(md):
      if stat.S_ISDIR(md):
        return 'd'
      elif stat.S_ISSOCK(md):
        return 's'
      else:
        return '-'

    def triple(md):
      return '%c%c%c' % (
        'r' if md & 0b100 else '-',
        'w' if md & 0b010 else '-',
        'x' if md & 0b001 else '-')

    return ''.join([stat_type(mode), triple(root), triple(group), triple(user)])

  def format_mtime(mtime):
    dt = datetime.fromtimestamp(mtime)
    return '%s %2d %5s' % (dt.strftime('%b'), dt.day,
      dt.year if dt.year != NOW.year else dt.strftime('%H:%M'))

  def format_prefix(filename, sres):
    try:
      pwent = pwd.getpwuid(sres.st_uid)
      user = pwent.pw_name
    except KeyError:
      user = sres.st_uid

    try:
      grent = grp.getgrgid(sres.st_gid)
      group = grent.gr_name
    except KeyError:
      group = sres.st_gid

    return '%s %3d %10s %10s %10d %s' % (
      format_mode(sres),
      sres.st_nlink,
      user,
      group,
      sres.st_size,
      format_mtime(sres.st_mtime),
    )
%>

<%def name="download_link(filename)"><a href='/download/${task_id}/${os.path.join(path, filename)}'><font size=1>dl</font></a></%def>
<%def name="directory_link(dirname)"><a href='/browse/${task_id}/${os.path.join(path, dirname)}'>${dirname}</a></%def>
<%def name="file_link(filename)"><a href='/file/${task_id}/${os.path.join(path, filename)}'>${filename}</a></%def>

<html>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>
<style type="text/css">
div.tight
{
  height:85%;
  overflow:auto;
}
</style>

<title>path browser for ${task_id}</title>


% if chroot is not None:
<body>
  <div class="container">
  <div class="span6">
    <strong> task id </strong> ${task_id}
  </div>
  <div class="span6">
    <strong> path </strong> ${path}
  </div>
  <div class="span12 tight">
    <pre>

% if path != ".":
  <%
     listing = ['..'] + os.listdir(os.path.join(chroot, path))
  %>\
% else:
  <%
     listing = os.listdir(os.path.join(chroot, path))
  %>\
% endif

<% listing.sort() %>

% for fn in listing:
<%
  try:
    sres = os.stat(os.path.join(chroot, path, fn))
  except OSError:
    continue
%>\
  % if not stat.S_ISDIR(sres.st_mode):
${format_prefix(fn, sres)} ${file_link(fn)} ${download_link(fn)}
  % else:
${format_prefix(fn, sres)} ${directory_link(fn)}
  % endif
% endfor
    </pre>
  </div>
  </div>
</body>
% else:
<body>
  This task is running without a chroot.
</body>
% endif

</html>
