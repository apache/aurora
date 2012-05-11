<html>
<title>thermos(${hostname})</title>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>

<body>

<%!
 import socket
 import time

 def pretty_time(seconds):
   return time.asctime(time.localtime(seconds))
%>

<div class="container">
  <h3> host ${socket.gethostname()} </h3>

  <div class="content" id="defaultLayout">
     <table class="zebra-striped">
     <thead>
       <tr>
         <th colspan=3> task </th>
         <th colspan=4> resources </th>
         <th colspan=3> links </th>
       </tr>

       <tr>
         <th> name </th> <th> role </th> <th> status </th>
         <th> procs </th> <th> cpu </th> <th> ram </th> <th> disk </th>
         <th> task </th> <th> chroot </th> <th> ports </th>
       </tr>
      </thead>
      <tbody>

      % for proc_name, proc in sorted(processes.items()):
       <tr>
         <td> ${proc["process_name"]} </td>
         <td> ${proc["process_run"]} </td>
         <td> ${proc["state"]} </td>
         <td> ${pretty_time(float(proc["start_time"])/1000.0) if "start_time" in proc else ""} </td>
         <td> ${pretty_time(float(proc["stop_time"])/1000.0) if "stop_time" in proc else ""} </td>
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
