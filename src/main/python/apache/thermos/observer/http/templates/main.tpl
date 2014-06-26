<!--
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

 <%doc>
 Template arguments:
  type
  offset
  num
  tasks
  task_count
</%doc>

<%!
import socket
import time

try:
  from twitter.common import app
  observer_port = app.get_options().twitter_common_http_root_server_port
except (ImportError, AttributeError) as e:
  observer_port = 1338

host = socket.gethostname()
num_tasks = 20

def pretty_time(seconds=time.time()):
  return time.strftime('%m/%d %H:%M:%S', time.gmtime(seconds))

%>

<div class="${type}-container"
     data-url="/main/${type}/${offset}/${num}">
  <div class="row-fluid">
    <div class="span2">
    % if offset > 0:
      <a class="refresh-container" href="#"
         data-url="/main/${type}/${offset-num_tasks}/${num}">
         &larr; newer
      </a>
    % endif
    </div>
    % if offset + num_tasks < task_count:
    <div class="span2">
      <a class="refresh-container" href="#"
         data-url="/main/${type}/${offset+num_tasks}/${num}">
         older &rarr;
      </a>
    </div>
    % endif
  </div>
  <div class="content" id="defaultLayout">
     <table class="table table-bordered table-condensed table-striped" style="empty-cells:show;">
     <thead>
       <tr>
         <th colspan=4> ${type} tasks ${offset}...${min(task_count, offset+num_tasks) - 1} of ${task_count} </th>
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

         <td> <a href="/task/${row['task_id']}">info</a> </td>
         <td> <a href="/browse/${row['task_id']}">browse</a> </td>
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
             <a href="/task/${row['task_id']}">...</a>
           % endif
         % endif
         </td>
       </tr>
      % endfor
     </tbody>
     </table>
  </div>
</div>
