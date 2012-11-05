<%doc>
 Template arguments:
  hostname
  task_id
  task_struct
</%doc>

<html>
<title>thermos(${hostname})</title>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>
<%!
  from json import dumps
  def print_task(task):
    return dumps(task.get(), indent=4)
%>

<body>
<div class="container">
  <h3> task ${task_id} </h3>
  <div class="content" id="rawTask">
    <pre>${print_task(task_struct)}</pre>
  </div>
</div>
</body>
</html>
