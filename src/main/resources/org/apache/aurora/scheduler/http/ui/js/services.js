'use strict';

auroraUI.factory(
  'auroraClient',
  function () {
    return {
      getJobSummary: function () {
        var client = this.makeSchedulerClient();

        var response;
        console.log("querying server");
        response = client.getJobSummary();
        console.log(response);
        return response.result.jobSummaryResult;
      },

      makeSchedulerClient: function () {
        var transport = new Thrift.Transport("/api/");
        var protocol = new Thrift.Protocol(transport);
        return new ReadOnlySchedulerClient(protocol);
      }
    };
  }
);
