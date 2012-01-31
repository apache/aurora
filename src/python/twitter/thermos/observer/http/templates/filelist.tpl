<%doc>
 Template arguments:
   task_id
   path
   dirs
   files
</%doc>

<%!
  import os
%>

<%def name="download_link(filename)">
  <a href='/download/${task_id}/${os.path.join(path, filename)}'><font size=1>dl</font></a>
</%def>

<%def name="directory_link(dirname)">
  <a href='/browse/${task_id}/${os.path.join(path, dirname)}'>${dirname}</a>
</%def>

<%def name="file_link(filename)">
  <a href='/file/${task_id}/${os.path.join(path, filename)}'>${filename}</a>
</%def>

<html>

<title>path browser for ${task_id}</title>
<body>
  <h3>${path}</h3>
  <div class="container">
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
  </div>
</body>

</html>
