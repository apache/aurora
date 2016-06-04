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
 <html>
  <head>
    <title>Aurora $cluster_name$ agents</title>
    <link href="/assets/bower_components/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet" />
  </head>
  <body>
    <h2>Agent Hosts</h2>
    <table class="table table-striped table-condensed table-hover">
      <thead>
        <th>Host
        <th>Agent ID
        <th>Maintenance mode
        <th>Attributes
      </thead>
      <tbody>
        $agents:{ agent |
        <tr>
          <td><a href="http://$agent.host$:1338">$agent.host$</a>
          <td>$agent.id$
          <td>$agent.mode$
          <td>$agent.attributes$
        </tr>
        }$
      </tbody>
    </table>
  </body>
</html>
