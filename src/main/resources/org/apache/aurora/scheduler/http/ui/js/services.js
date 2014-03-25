'use strict';

auroraUI.factory(
  'auroraClient',
  function () {
    return {
      getRoleSummary: function () {
        var response = this.getSchedulerClient().getRoleSummary();
        return {
          error: response.responseCode !== 1,
          errorMsg: response.message,
          summaries: response.result !== null ? response.result.roleSummaryResult.summaries : [],
          pageTitle: this.getPageTitle(response.serverInfo)
        }
      },

      // TODO(Suman Karumuri): Make schedulerClient a service
      schedulerClient: null,

      getSchedulerClient: function () {
        if (!this.schedulerClient) {
          var transport = new Thrift.Transport("/api");
          var protocol = new Thrift.Protocol(transport);
          this.schedulerClient = new ReadOnlySchedulerClient(protocol);
          return this.schedulerClient;
        } else {
          return this.schedulerClient;
        }
      },

      getPageTitle: function (info) {
        var title = "Aurora UI";
        return info.error || typeof info.clusterName === "undefined"
                 ? title
                 : info.clusterName + " " + title;
      }
    };
  }
);
