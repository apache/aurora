'use strict';

auroraUI.factory(
  'auroraClient',
  function () {
    return {
      getJobSummary: function () {
        var response = this.makeSchedulerClient().getRoleSummary();
        return {
          error : response.responseCode !== 1,
          errorMsg : response.message,
          summaries : response.result !== null ? response.result.roleSummaryResult.summaries : []
        }
     },

      makeSchedulerClient: function () {
        var transport = new Thrift.Transport("/api/");
        var protocol = new Thrift.Protocol(transport);
        return new ReadOnlySchedulerClient(protocol);
      }
    };
  }
);
