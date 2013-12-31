/**
 * Dictionary to use for attaching tool-tips to various terminologies in Aurora.
 */
var dictionary = {
  states: {
    PENDING: 'The scheduler is searching for a machine that satisfies the resources and '
      + 'constraints for this task.',

    ASSIGNED: 'The scheduler has selected a machine to run the task and is instructing the '
      + 'slave to launch it.',

    STARTING: 'The executor is preparing to launch the task.',
    RUNNING: 'The user process(es) are running.',
    FAILED: 'The task ran, but did not exit indicating success.',
    FINISHED: 'The task ran and exited successfully.',
    KILLED: 'A user or cron invocation terminated the task.',
    PREEMPTING: 'This task is being killed to make resources available for a production task.',
    KILLING: 'A user request or cron invocation has requested the task be killed.',
    LOST: 'The task cannot be accounted for, usually a result of slave process or machine failure.'
  }
};
