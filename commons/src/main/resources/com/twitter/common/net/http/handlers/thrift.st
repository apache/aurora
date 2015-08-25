<html>
  <body>
    <h2>Clients</h2>
    $if(clientMonitors)$
      $clientMonitors:{ monitor |
        <div>
          <h3>$monitor.serviceName$</h3>
          Lifetime Requests: $monitor.lifetimeRequestCount$
          <br />
          <table border=0>
            <tr>
              <th>Server</th>
              <th>Leased Connections</th>
              <th>Successful Requests</th>
              <th>Failed Requests</th>
            </tr>
            $monitor.trafficInfo:{ trafficInfo |
              <tr>
                <td align='center'>$trafficInfo.key$</td>
                <td align='center'>$trafficInfo.connectionCount$</td>
                <td align='center'>$trafficInfo.requestSuccessCount$</td>
                <td align='center'>$trafficInfo.requestFailureCount$</td>
              </tr>
            }$
          </table>
        </div>
      }$
    $else$
      No thrift clients registered.
    $endif$

    <br />
    <br />

    <h2>Servers</h2>
    $if(serverMonitors)$
      $serverMonitors:{ monitor |
        <div>
          <h3>$monitor.serviceName$</h3>
          Lifetime Requests: $monitor.lifetimeRequestCount$
          <br />
          <table border=0>
            <tr>
              <th>Client</th>
              <th>Active Connections</th>
              <th>Successful Requests</th>
              <th>Failed Requests</th>
            </tr>
            $monitor.trafficInfo:{ trafficInfo |
              <tr>
                <td align='center'>$trafficInfo.key$</td>
                <td align='center'>$trafficInfo.connectionCount$</td>
                <td align='center'>$trafficInfo.requestSuccessCount$</td>
                <td align='center'>$trafficInfo.requestFailureCount$</td>
              </tr>
            }$
          </table>
        </div>
      }$
    $else$
      No thrift servers registered.
    $endif$
  </body>
</html>
