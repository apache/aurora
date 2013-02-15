<%doc>
 Template arguments:
  task_id
  task
  statuses
  user
  ports
  processes
  chroot
  launch_time
  hostname
</%doc>

<html>
<title>thermos(${hostname})</title>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>

<body>

<%!
 import time

 def pretty_time(seconds=time.time()):
   return time.strftime('%m/%d %H:%M:%S', time.gmtime(seconds))

 def get(task, typ):
   return task['resource_consumption'][typ]

%>

<div class="container">

  <h3> task ${task_id} </h3>

  <div class="row">
    <div class="span6" id="leftBar">
      <dl>
        <dt> task </dt>
          <dd> <strong> status </strong> ${statuses[-1][0] if statuses else 'UNKNOWN'} </dd>
          <dd> <strong> user </strong> ${user} </dd>
        <dt> ports </dt>
        % for port_name, port_number in ports.items():
          <dd> <strong> ${port_name} </strong> <a href="http://${hostname}:${port_number}">${port_number}</a> </dd>
        %endfor
      </dl>
    </div>

    <div class="span6" id="rightBar">
      <dl>
        <dt> header </dt>
          <dd> <strong> chroot </strong> <a href="/browse/${task_id}">browse</a> </dd>
          <dd> <strong> hostname </strong> <a href="/">${hostname}</a> </dd>
          <dd> <strong> launch time </strong> ${pretty_time(launch_time)} </dd>
          <dd> <strong> task config </strong> <a href="/rawtask/${task_id}">view</a> </dd>
      </dl>
    </div>
  </div>

  <div class="row-fluid">
    <div class="span8" id="taskLayout">
       <table class="table table-bordered table-condensed table-striped" style="empty-cells:show;">
       <thead>
         <tr>
           <th colspan=1> task status </th>
           <th colspan=1> time </th>
         </tr>
        </thead>

        <tbody>
        % for status, timestamp in sorted(statuses, key=lambda status: status[1]):
         <tr>
           <td> ${status} </td> <td> ${pretty_time(timestamp)} </td>
         </tr>
        % endfor
       </tbody>
       </table>
    </div>

    <div class="span4" id="taskResources">
       <table class="table table-bordered table-condensed table-striped" style="empty-cells:show;">
       <thead>
         <tr>
           <th> cpu </th>
           <th> ram </th>
           <th> disk </th>
         </tr>
        </thead>

        <tbody>
         <tr>
           <td> ${'%.3f' % get(task, 'cpu')} </td>
           <td> ${'%.1fMB' % (get(task, 'ram') / 1024. / 1024.)} </td>
           <td> ${'%.1fGB' % (get(task, 'disk') / 1024. / 1024. / 1024.)} </td>
         </tr>
       </tbody>
       </table>
    </div>
  </div>

  <div class="content" id="processesLayout">
     <table class="table table-bordered table-condensed table-striped" style="empty-cells:show;">
     <thead>
       <tr>
         <th colspan=3> process </th>
         <th colspan=2> time </th>
         <th colspan=2> used </th>
         <th colspan=2> logs </th>
       </tr>

       <tr>
         <th> name </th> <th> run </th> <th> status </th> <th> started </th> <th> finished </th>
         <th> cpu </th> <th> ram </th>
         <th> stdout </th> <th> stderr </th>
       </tr>
      </thead>

      <tbody>
      % for proc_name, proc in sorted(processes.items(), key=lambda item: item[1].get('start_time')):
       <tr>
         <td> <a href="/process/${task_id}/${proc["process_name"]}">${proc["process_name"]}</a> </td>
         <td> ${proc["process_run"]} </td>
         <td> ${proc["state"]} </td>
         <td> ${pretty_time(proc["start_time"]) if "start_time" in proc else ""} </td>
         <td> ${pretty_time(proc["stop_time"]) if "stop_time" in proc else ""} </td>
         <td> ${'%.3f' % proc["used"]["cpu"] if "used" in proc else ""} </td>
         <td> ${'%dMB' % (proc["used"]["ram"] / 1024 / 1024) if "used" in proc else ""} </td>
         <td> <a href="/logs/${task_id}/${proc["process_name"]}/${proc["process_run"]}/stdout">stdout</a> </td>
         <td> <a href="/logs/${task_id}/${proc["process_name"]}/${proc["process_run"]}/stderr">stderr</a> </td>
       </tr>
      % endfor
     </tbody>
     </table>
  </div>
</div>

</body>
</html>
