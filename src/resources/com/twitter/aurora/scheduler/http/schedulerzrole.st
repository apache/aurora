<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Aurora $cluster_name$ scheduler - Jobs for $role$</title>
    <link href="/css/bootstrap.min.css" rel="stylesheet" />
    <link href="/css/jquery.dataTables.css" rel="stylesheet" />
    <script type="text/javascript" src="/js/jquery.min.js"></script>
    <script type="text/javascript" src="/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="/js/util.js"></script>
    <script type="text/javascript" src="/js/jquery.dataTables.min.js"></script>
    <script type="text/javascript" src="/js/dataTables.bootstrap.js"></script>
    <script type="text/javascript" src="/js/dataTables.localstorage.js"></script>
  </head>
  <body>
    <div class="container-fluid">
      <div class="row-fluid">
        <center>
          <h2>Jobs for role $role$</h2>
        </center>
      </div>

      <div class="row-fluid">
        <ul class="breadcrumb">
          <li>
            <a href="/scheduler">Home</a> <span class="divider">></span>
          </li>
          <li class="active">Role: $role$</li>
        </ul>
      </div>

      $if(jobs)$
      <div class="row-fluid" style="margin-bottom: 20px;">
        <button id="prod-resources-toggle-btn" class="btn">
          <i class="icon-plus" style="margin-right: 3px;"></i><span class="btn-text">Show Production Resources</span>
        </button>
        <div id="prod-resources" class="collapse" style="width: 400px; margin-top: 10px;">
          <table class="table table-striped table-condensed table-hover">
            <thead>
              <th>Resource
              <th>Used
              <th>Quota
            </thead>
            <tbody>
              <tr>
                <td>CPU
                <td>$resourcesUsed.numCpus$
                <td>$resourceQuota.numCpus$
              </tr>
              <tr>
                <td>RAM
                <td>$resourcesUsed.ramMb$
                <td>$resourceQuota.ramMb$
              </tr>
              <tr>
                <td>Disk
                <td>$resourcesUsed.diskMb$
                <td>$resourceQuota.diskMb$
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div class="row-fluid" style="margin-bottom: 20px;">
        <table id="jobs-table" class="table table-bordered table-striped table-condensed table-hover">
          <thead>
            <th>Job
            <th>Pending Tasks
            <th>Active Tasks
            <th>Finished Tasks
            <th>Failed Tasks
          </thead>
          <tbody>
            $jobs:{ job |
            <tr>
              <td>
                <a href="/scheduler/$role$/$job.environment$/$job.name$">$job.environment$/$job.name$</a>
                <span class="job-status pull-right label"></span>
              <td>
                <a href="/scheduler/$role$/$job.environment$/$job.name$?status=pending">$job.pendingTaskCount$</a>
                <input type="hidden" class="pending-tasks" value="$job.pendingTaskCount$" />
              <td>
                <a href="/scheduler/$role$/$job.environment$/$job.name$?status=running">$job.activeTaskCount$</a>
                <input type="hidden" class="active-tasks" value="$job.activeTaskCount$" />
              <td>
                <a href="/scheduler/$role$/$job.environment$/$job.name$?status=finished">$job.finishedTaskCount$</a>
                <input type="hidden" class="finished-tasks" value="$job.finishedTaskCount$" />
              <td>
                <a href="/scheduler/$role$/$job.environment$/$job.name$?status=failed">$job.failedTaskCount$</a>
                <input type="hidden" class="failed-tasks" value="$job.failedTaskCount$" />
                <input type="hidden" class="recently-failed-tasks"
                    value="$job.recentlyFailedTaskCount$" />
            </tr>
            }$
          </tbody>
        </table>
      </div>
      $endif$

      $if(cronJobs)$
      <div class="row-fluid">
        <h4>Cron jobs</h4>
        <div>
          <table id="cron-jobs-table" class="table table-bordered table-striped table-condensed table-hover">
            <thead>
              <th>Name
              <th>Tasks
              <th>Schedule
              <th>Next Run
              <th>Collision policy
              <th>Packages
            </thead>
            <tbody>
              $cronJobs:{ job |
              <tr>
                <td>$job.environment$/$job.name$
                <td>$job.pendingTaskCount$
                <td>$job.cronSchedule$
                <td><script>document.write(printDate($job.nextRun$));</script>
                <td>$job.cronCollisionPolicy$
                <td>$job.packages$
              </tr>
              }$
            </tbody>
          </table>
        </div>
      </div>
      $endif$
    </div>

    $if(exception)$
      <font color='red'>Exception: $exception$</font>
    $endif$

    <script type="text/javascript">
      jQuery(function(){
        jQuery('#jobs-table').dataTable({
          'bStateSave': true,
          'aoColumns': [
            {'bSearchable': true},
            {'bSearchable': false, 'sType': 'num-html'},
            {'bSearchable': false, 'sType': 'num-html'},
            {'bSearchable': false, 'sType': 'num-html'},
            {'bSearchable': false, 'sType': 'num-html'}
          ]
        });
        jQuery('#cron-jobs-table').dataTable({
          'bStateSave': true,
          'aoColumns': [
            {'bSearchable': true},
            {'bSearchable': false},
            {'bSearchable': false, 'bSortable': false},
            {'bSearchable': false},
            {'bSearchable': true},
            {'bSearchable': true, 'bSortable': true}
          ]
        });

        // Bind click event to button
        jQuery('#prod-resources-toggle-btn').on('click', function(e){
          e.stopPropagation();
          e.preventDefault();
          jQuery('#prod-resources').collapse('toggle');
        });

        // Switch between + and - icons based on button state
        jQuery('#prod-resources').on('show', function(){
          var parent = jQuery(this).parent();
          var icon = parent.find('i');
          var buttonText = parent.find('.btn-text');

          icon.addClass('icon-minus').removeClass('icon-plus');
          buttonText.html('Hide Production Resources');
        });

        jQuery('#prod-resources').on('hide', function(){
          var parent = jQuery(this).parent();
          var icon = parent.find('i');
          var buttonText = parent.find('.btn-text');

          icon.addClass('icon-plus').removeClass('icon-minus');
          buttonText.html('Show Production Resources');
        });

        // Update job health
        jQuery('#jobs-table>tbody').find('tr').each(function(){
          var tr = jQuery(this);
          var activeTasks = Number(tr.find('.active-tasks').val());
          var recentlyFailedTasks = Number(tr.find('.recently-failed-tasks').val());
          var pendingTasks = Number(tr.find('.pending-tasks').val());
          var finishedTasks = Number(tr.find('.finished-tasks').val());
          var totalTasks = pendingTasks + activeTasks + finishedTasks;
          var jobStatus = tr.find('.job-status');

          if (recentlyFailedTasks >= (totalTasks / 2)) {
            jobStatus.addClass('label-important').html('Unhealthy');
            jobStatus.attr('title', 'More than half of total tasks have failed in last 6 hours');
          } else if (recentlyFailedTasks > 0) {
            jobStatus.addClass('label-warning').html('Warning');
            jobStatus.attr('title', 'One or more tasks have failed in last 6 hours');
          } else {
            jobStatus.addClass('label-success').html('Healthy');
            jobStatus.attr('title', 'No failed tasks in last 6 hours');
          }

          jobStatus.tooltip();
        });
      });
    </script>
  </body>
</html>
