<html>
<title>thermos(${hostname})</title>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>

<body>



<%!
 import time
%>

<div class="container">

  <h3> task ${uid} </h3>

  <div class="row">
    <div class="span8" id="leftBar">
      <dl>
        <dt> task </dt>
          <dd> <strong> status </strong> ${status} </dd>
          <dd> <strong> role </strong> ${role} </dd>
          <dd> <strong> user </strong> ${user} </dd>
          <dd> <strong> job </strong> ${job} </dd>
          <dd> <strong> replica </strong> ${replica} </dd>
      </dl>
    </div>

    <div class="span8" id="rightBar">
      <dl>
        <dt> executor </dt>
          <dd> <strong> id </strong> ${executor_id} </dd>
          <dd> <strong> chroot </strong> ${chroot} </dd>
          <dd> <strong> cluster </strong> ${cluster} </dd>
      </dl>
    </div>
  </div>

  <div class="content" id="defaultLayout">
     <table class="zebra-striped">
     <thead>
       <tr>
         <th colspan=3> process </th>
         <th colspan=2> time </th>
         <th colspan=2> reserved </th>
         <th colspan=2> used </th>
         <th colspan=2> logs </th>
       </tr>

       <tr>
         <th> name </th> <th> run </th> <th> status </th> <th> started </th> <th> finished </th>
         <th> cpu </th> <th> ram </th>
         <th> cpu </th> <th> ram </th>
         <th> stdout </th> <th> stderr </th>
       </tr>
      </thead>
      <tbody>

      % for proc_name, proc in sorted(processes.items()):
       <tr>
         <td> ${proc["process_name"]} </td>
         <td> ${proc["process_run"]} </td>
         <td> ${proc["state"]} </td>
         <td> ${time.asctime(time.localtime(float(proc["start_time"])/1000.0)) if "start_time" in proc else ""} </td>
         <td> ${time.asctime(time.localtime(float(proc["stop_time"])/1000.0)) if "stop_time" in proc else ""} </td>
         <td> ${proc["reserved"]["cpu"]} </td>
         <td> ${'%dMB' % (proc["reserved"]["ram"] / 1048576)} </td>
         <td> ${'%.3f' % proc["used"]["cpu"] if "used" in proc else ""} </td>
         <td> ${'%dMB' % (proc["used"]["ram"] / 1048576) if "used" in proc else ""} </td>
         <td> <a href="/logs/${uid}/${proc["process_name"]}/${proc["process_run"]}/stdout">stdout</a> </td>
         <td> <a href="/logs/${uid}/${proc["process_name"]}/${proc["process_run"]}/stderr">stderr</a> </td>
       </tr>
      % endfor
     </tbody>
     </table>
  </div>

</div>

</body>
</html>
