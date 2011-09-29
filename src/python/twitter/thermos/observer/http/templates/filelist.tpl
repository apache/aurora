<%doc>
 Template arguments:
   uid
   path
   dirs
   files
</%doc>

<%!
  import os
%>

<%def name="download_link(filename)">
  <a href='/download/${uid}/${os.path.join(path, filename)}'><font size=1>dl</font></a>
</%def>

<%def name="directory_link(dirname)">
  <a href='/browse/${uid}/${os.path.join(path, dirname)}'>${dirname}</a>
</%def>

<%def name="file_link(filename)">
  <a href='/file/${uid}/${os.path.join(path, filename)}'>${filename}</a>
</%def>

<html>

<title>Log Printer</title>
<body>
  <h3>task ${uid}, path ${path}</h3>
  <table border=0 cellpadding=0 cellspacing=5 align=left>
    % for dir in dirs:
      <tr>
       <td></td>
       <td> ${directory_link(dir)} </td>
      </tr>
    % endfor
    % for fn in files:
      <tr>
       <td> ${download_link(fn)} </td>
       <td> ${file_link(fn)} </td>
      </tr>
    % endfor
  </table>
</body>

</html>
