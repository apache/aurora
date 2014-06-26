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
    <pre>${print_task(task_struct) | h}</pre>
  </div>
</div>
</body>
</html>
