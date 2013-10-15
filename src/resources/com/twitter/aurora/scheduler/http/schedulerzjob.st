<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Aurora $cluster_name$ scheduler - Tasks for $role$/$environment$/$job$</title>
    <link href="/css/bootstrap.min.css" rel="stylesheet" />
    <script type="text/javascript" src="/js/jquery.min.js"></script>
    <script type="text/javascript" src="/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="/js/util.js"></script>
    <script type="text/javascript" src="/js/dictionary.js"></script>
    <style>
      .expand-btn:hover{
        text-decoration: none;
      }
      .task-status {
        cursor: help;
        border-bottom: 1px #aaaaaa dashed;
      }
    </style>
  </head>
  <body>
    <div class="container-fluid">
      <div class="row-fluid">
        <div style="float:right; position:absolute; right:20px">
          <a href="$statsUrl$"><img src="/images/viz.png" alt="Container stats on viz" /></a>
        </div>
        <center>
          <h2>Job: $job$</h2>
          <h5 style='margin-top: -12px;'>Role: $role$ - Environment: $environment$</h5>
        </center>
      </div>

      <div class="row-fluid">
        <ul class="breadcrumb">
          <li>
            <a href="/scheduler">Home</a> <span class="divider">></span>
          </li>
          <li>
            <a href="/scheduler/$role$">Role: $role$</a> <span class="divider">></span>
          </li>
          <li>
            <a href="/scheduler/$role$/$environment$">Environment: $environment$</a>
            <span class="divider">></span>
          </li>
          <li class="active">Job: $job$</li>
        </ul>
      </div>

      $if(admin)$
        $if(is_cron)$
          (<a href="/structdump/cron/$role$/$environment$/$job$">dump</a>)
        $endif$
      $endif$


      $if(schedulingDetails)$
        <div class="row-fluid" style="margin-bottom: 20px;">
          <button id="scheduling-details-toggle-btn" class="btn">
            <i class="icon-plus" style="margin-right: 3px;"></i>
            <span class="btn-text">Show Details</span>
          </button>
          <div  id="scheduling-details" class="collapse" style="width: 400px; margin-top: 10px;">
            <table class="table table-condensed" style="margin-bottom: 0px;">
              <thead>
                <th>Instances
                <th>Details
              </thead>
              <tbody>
                $schedulingDetails.keys:{ instances |
                <tr>
                  <td style='text-align:center; vertical-align:middle'>$instances$
                  <td>
                    <table class="table table-condensed" style="margin-bottom: 0px;">
                      <tbody>
                      $schedulingDetails.(instances).details.keys: { name |
                        <tr>
                          <td>$name$
                          <td>$schedulingDetails.(instances).details.(name)$
                        </tr>
                      }$
                      </tbody>
                    </table>
                </tr>
                }$
              </tbody>
            </table>
          </div>
        </div>
      $endif$

      <script>
        // The current Date, used in calls to printElapsed().
        var nowTimestamp = new Date().getTime();
      </script>

      $if(activeTasks)$
        <div style='clear:both'>
          <h3>Active Tasks</h3>
          <table class="table table-striped table-condensed table-hover">
            <thead>
              <th>Instance
              <th>Status
              $if(admin)$
              <th>Task ID
              $endif$
              <th>Host
            </thead>
            <tbody>
              $activeTasks:{ task |
              <tr>
                <td style='vertical-align:middle'>
                  $task.instanceId$ $task.links.keys, task.links.values: { k, v | <a href='$v$'>$k$</a> }$
                <td style='vertical-align:middle'>
                  <a href='#accordion-$task.taskId$' class='expand-btn' title='Show details'>
                    <i class='icon-plus'></i>
                  </a>
                  <script>
                    document.write(printElapsed(nowTimestamp, $task.statusTimestamp$) + ' ago ');
                  </script>
                  - <span class='task-status' status='$task.status$'>$task.status$ </span>
                  $if(task.pendingReason)$ ($task.pendingReason$)$endif$
                  <div class='collapse' id='accordion-$task.taskId$' style='margin-bottom: 0px'>
                    <ul>
                      $task.taskEvents:{ event |
                      <li>
                        <script>document.write(printDate($event.timestamp$));</script>
                        - <span class='task-status' status='$event.status$'>$event.status$</span>
                        $if(event.message)$: $event.message$$endif$
                      </li>
                      }$
                    </ul>
                  </div>
                $if(admin)$
                <td><a href='/structdump/task/$task.taskId$'>$task.taskId$</a>
                $endif$
                <td style='vertical-align:middle'>
                  $if(task.slaveHost)$ <a href='$task.executorUri$'>$task.slaveHost$</a> $endif$
              </tr>
              }$
            </tbody>
          </table>
        </div>
      $endif$

      <br />
      <br />

      $if(completedTasks)$
        <h3>Completed Tasks</h3>
        <table class="table table-striped table-condensed table-hover">
          <thead>
            <th>Instance
            <th>Status
            <th>Running duration
            $if(admin)$
            <th>Task ID
            $endif$
            <th>Host
          </thead>
          <tbody>
          $completedTasks:{ task |
          <tr>
            <td>$task.instanceId$
            <td style='vertical-align:middle'>
              <a href='#collapse-$task.taskId$' class='expand-btn' title='Show details'>
                <i class='icon-plus'></i>
              </a>
              <script>
                document.write(printElapsed(nowTimestamp, $task.statusTimestamp$) + ' ago ');
              </script>
              - <span class='task-status' status='$task.status$'>$task.status$</span>
              $last(task.taskEvents):{ event |
                $if(event.message)$
                  : $event.message$
                $endif$
              }$

              <div id='collapse-$task.taskId$' class='collapse'>
                <ul>
                  $task.taskEvents:{ event |
                  <li>
                    <script>document.write(printDate($event.timestamp$));</script>
                    - <span class='task-status' status='$event.status$'>$event.status$</span>
                    $if(event.message)$: $event.message$$endif$
                  </li>
                  }$
                </ul>
              </div>
            <td>
              <script>
                var events = [
                  $task.taskEvents:{ event | [ '$event.status$', $event.timestamp$ ], }$
                ];
                var aliveTimestamp = 0;
                for (var i = events.length - 1; i >= 0; i--) {
                  if (events[i][0] == 'RUNNING') {
                    aliveTimestamp = events[i][1];
                    break;
                  }
                }
                if (aliveTimestamp != 0) {
                  document.write(printElapsed(events[events.length - 1][1], aliveTimestamp));
                }
              </script>
            $if(admin)$
            <td><a href='/structdump/task/$task.taskId$'>$task.taskId$</a>
            $endif$
            <td>$if(task.slaveHost)$ <a href='$task.executorUri$'>$task.slaveHost$</a> $endif$
          </tr>
          }$
          </tbody>
        </table>
      $endif$

      $if(exception)$
        <font color='red'>Exception: $exception$</font>
      $endif$

      <div class='pagination' style='text-align: center'>
        <ul>
          <li $if(!prevOffset)$ class='disabled' $endif$>
          $if(prevOffset)$
            <a href='/scheduler/$role$/$environment$/$job$?$if(status)$status=$status$$endif$&o=$prevOffset$'>&laquo;</a>
          $else$
            <span>&laquo;</span>
          $endif$
          </li>
          <li $if(!nextOffset)$ class='disabled' $endif$>
          $if(nextOffset)$
            <a href='/scheduler/$role$/$environment$/$job$?$if(status)$status=$status$$endif$&o=$nextOffset$'>&raquo;</a>
          $else$
            <span>&raquo;</span>
          $endif$
          </li>
        </ul>
      </div>
    </div>

    <script type="text/javascript">
      jQuery(function(){
        // Enable tooltips on all expansion buttons
        jQuery('.expand-btn').tooltip();

        // Bind click event to all expand buttons
        jQuery('.expand-btn').on('click', function(e){
          e.stopPropagation();
          e.preventDefault();

          jQuery(jQuery(this).attr('href')).collapse('toggle');
        });

        // Switch between + and - icons based on button state
        jQuery('div.collapse').on('show', function(){
          var parent = jQuery(this).parent();
          var icon = parent.find('i');
          var button = parent.find('.expand-btn');

          icon.addClass('icon-minus').removeClass('icon-plus');
          button.attr('data-original-title', 'Hide details').tooltip('hide');
        });

        jQuery('div.collapse').on('hide', function(){
          var parent = jQuery(this).parent();
          var icon = parent.find('i');
          var button = parent.find('.expand-btn');

          icon.addClass('icon-plus').removeClass('icon-minus');
          button.attr('data-original-title', 'View details');
        });

        // Attach tooltips to task states
        jQuery('.task-status').each(function() {
          jQuery(this).popover({
            placement: 'right',
            trigger: 'hover',
            title: jQuery(this).attr('status'),
            content: dictionary.states[jQuery(this).attr('status')]
          });
        });
      });

      // Bind click event to button
      jQuery('#scheduling-details-toggle-btn').on('click', function(e){
        e.stopPropagation();
        e.preventDefault();
        jQuery('#scheduling-details').collapse('toggle');
      });

      // Switch between + and - icons based on button state
      jQuery('#scheduling-details').on('show', function(){
        var parent = jQuery(this).parent();
        var icon = parent.find('i');
        var buttonText = parent.find('.btn-text');

        icon.addClass('icon-minus').removeClass('icon-plus');
        buttonText.html('Hide Details');
      });

      jQuery('#scheduling-details').on('hide', function(){
        var parent = jQuery(this).parent();
        var icon = parent.find('i');
        var buttonText = parent.find('.btn-text');

        icon.addClass('icon-plus').removeClass('icon-minus');
        buttonText.html('Show Details');
      });
    </script>
  </body>
  <div style='bottom: 10px; right: 10px; position: fixed;'>
    <a href='/scheduler/$role$/$environment$/$job$?admin$if(status)$&status=$status$$endif$&o=$offset$'>&pi;</a>
  </div>
</html>

