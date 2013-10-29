/**
 * Make browser's local storage as the default storage for Datatables plugin.
 * Compatible with Datatables v1.9.4
 *
 * http://www.datatables.net/beta/1.9/examples/advanced_init/localstorage.html
 *
 * Always link this file after linking jquery.dataTables.min.js
 */
$.extend(true, $.fn.dataTable.defaults, {    
  "fnStateSave": function (oSettings, oData) {
  localStorage.setItem("DataTables_" + window.location.pathname + "_" + oSettings.sTableId, 
                       JSON.stringify(oData));
  },
  "fnStateLoad": function (oSettings) {
    return JSON.parse(localStorage
        .getItem("DataTables_" + window.location.pathname + "_" + oSettings.sTableId));
  }
});