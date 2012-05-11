<%doc>
Template arguments:
  task_id
  process {
    cpu:/ram: (optional)
    cmdline:
    name:
  }

  runs = {
   number: {
     start_time: (optional)
     stop_time: (optional)
     state:
   }
  }

  --
  for each run:
     run | state | started | finished | stdout | stderr
</%doc>

<%!
  import socket
%>

<html>
<title>thermos(${socket.gethostname()})</title>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>
<body>

<%!
 import time

 def pretty_time(seconds):
   return time.asctime(time.localtime(float(seconds)/1000.0))
%>

<div class="container">
  <div class="row">
    <div class="span6" id="leftBar">
      <dl>
        <dt> process </dt>
          <dd> <strong> parent task </strong> <a href="/task/${task_id}">${task_id}</a> </dd>
          <dd> <strong> process name </strong> ${process["name"]} </dd>
          <dd> <strong> status </strong> ${process["status"]} </dd>
      </dl>
    </div>

    <div class="span6" id="rightBar">
      <dl>
        <dt> resources </dt>
          <dd> <strong> cpu </strong> ${'%.3f' % process["cpu"] if "cpu" in process else "N/A"} </dd>
          <dd> <strong> ram </strong> ${'%.1fMB' % (process["ram"] / 1024. / 1024.) if "ram" in process else "N/A"} </dd>
          <dd> <strong> total user </strong> ${'%.1fs' % process["user"] if "user" in process else "N/A"} </dd>
          <dd> <strong> total sys </strong> ${'%.1fs' % process["system"] if "system" in process else "N/A"} </dd>
          <dd> <strong> threads </strong> ${process["threads"] if "threads" in process else "N/A"} </dd>
      </dl>
    </div>
  </div>


  <strong> cmdline </strong><br>
  <div class="container">
<pre>
${process["cmdline"]}
</pre>
  </div><br><br>


  <strong> runs </strong>
  <div class="container">
     <table class="table table-bordered table-condensed table-striped">
     <thead>
       <tr>
         <th colspan=2> </th>
         <th colspan=2> time </th>
         <th colspan=2> logs </th>
       </tr>

       <tr>
         <th> run </th>
         <th> status </th>
         <th> started </th> <th> finished </th>
         <th> stdout </th> <th> stderr </th>
       </tr>
      </thead>
      <tbody>

      % for run, process_dict in sorted(runs.items(), reverse=True):
       <tr>
         <td> ${run} </td>
         <td> ${process_dict["state"]} </td>
         <td> ${pretty_time(process_dict["start_time"]) if "start_time" in process_dict else ""} </td>
         <td> ${pretty_time(process_dict["stop_time"]) if "stop_time" in process_dict else ""} </td>
         <td> <a href="/logs/${task_id}/${process["name"]}/${run}/stdout">stdout</a> </td>
         <td> <a href="/logs/${task_id}/${process["name"]}/${run}/stderr">stderr</a> </td>
       </tr>
      % endfor
     </tbody>
     </table>
  </div>

</div>

</body>
</html>
