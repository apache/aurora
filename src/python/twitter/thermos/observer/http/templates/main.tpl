<%doc>
 Template arguments:
  type
  offset
  num
  tasks
</%doc>

<%!
import socket
import time

host = socket.gethostname()
observer_port = 1338
num_tasks = 20

def pretty_time(seconds=time.time()):
  return time.strftime('%m/%d %H:%M:%S', time.gmtime(seconds))
%>

<div class="${type}-container"
     data-url="http://${host}:${observer_port}/main/${type}/${offset}/${num}">
  <div class="row-fluid">
    <div class="span2">
      <a class="refresh-container" href="#"
         data-url="http://${host}:${observer_port}/main/${type}/${offset-num_tasks}/${num}">
         &larr; newer
      </a>
    </div>
    <div class="span2">
      <a class="refresh-container" href="#"
         data-url="http://${host}:${observer_port}/main/${type}/${offset+num_tasks}/${num}">
         older &rarr;
      </a>
    </div>
  </div>
  <div class="content" id="defaultLayout">
     <table class="table table-bordered table-condensed table-striped">
     <thead>
       <tr>
         <th colspan=4> ${type} tasks ${offset}...${offset+num_tasks-1} </th>
         <th colspan=3> resources </th>
         <th colspan=3> links </th>
       </tr>

       <tr>
         <th> name </th> <th> role </th> <th> launched </th> <th> status </th>
         <th> cpu </th> <th> ram </th> <th> disk </th>
         <th> task </th> <th> chroot </th> <th> ports </th>
       </tr>
      </thead>
      <tbody>

      % for row in tasks:
       <tr>
         <td> ${row["name"]} </td>
         <td> ${row["role"]} </td>
         <td> ${pretty_time(row["launch_timestamp"])} </td>
         <td> ${row["state"]} @
              ${pretty_time(row["state_timestamp"]) if row["state_timestamp"] else ""}</td>

         <td> ${'%.3f' % row["cpu"] if row["cpu"] > 0 else ""} </td>
         <td> ${'%.1fMB' % (row["ram"] / 1024. / 1024.) if row["ram"] > 0 else ""} </td>
         <td> ${'%dGB' % (row["disk"] / 1024 / 1024 / 1024) if row["disk"] > 0 else ""} </td>

         <td> <a href="http://${host}:${observer_port}/task/${row['task_id']}">info</a> </td>
         <td> <a href="http://${host}:${observer_port}/browse/${row['task_id']}">browse</a> </td>
         <td>
         % if type == 'active':
           % if 'http' in row["ports"]:
             <a href="http://${host}:${row['ports']['http']}">http</a>
           % else:
             <span class="muted">http</span>
           % endif
           % if 'admin' in row["ports"]:
             <a href="http://${host}:${row['ports']['admin']}">admin</a>
           % else:
             <span class="muted">admin</span>
           % endif
           % if set(row["ports"]) - set(['http', 'admin']):
             <a href="http://${host}:${observer_port}/task/${row['task_id']}">...</a>
           % endif
         % endif
         </td>
       </tr>
      % endfor
     </tbody>
     </table>
  </div>
</div>
