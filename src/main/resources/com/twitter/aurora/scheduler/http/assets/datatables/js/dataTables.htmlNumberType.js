/**
 * DataTables custom plugin to extract numbers from HTML tags
 * for sorting rows in numeric order.
 *
 * Ref: http://www.datatables.net/plug-ins/sorting
 *
 * Compatible with DataTables v1.9.4
 *
 */

jQuery.extend(jQuery.fn.dataTableExt.oSort, {
  "num-html-pre": function (a) {
    // Extract the numeric value from the html by removing all tags.
    var x = String(a).replace(/<[\s\S]*?>/g, "");
    return parseFloat(x);
  },
 
  "num-html-asc": function (a, b) {
    // Compare the extracted numeric values for asc sort.
    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
  },
 
  "num-html-desc": function (a, b) {
    // Compare the extracted numeric values for desc sort.
    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
  }
});

/*
 * Type detection function for custom type "num-html".
 *
 * Ref: http://www.datatables.net/plug-ins/type-detection#how_to
 */
jQuery.fn.dataTableExt.aTypes.unshift(function (sData) {
  sData = typeof sData.replace == "function" ?
    sData.replace( /<[\s\S]*?>/g, "" ) : sData;
  sData = $.trim(sData);
    
  var sValidFirstChars = "0123456789-";
  var sValidChars = "0123456789.";
  var Char;
  var bDecimal = false;
    
  // Check for a valid first char (no period and allow negatives).
  Char = sData.charAt(0); 
  if (sValidFirstChars.indexOf(Char) == -1) {
    return null;
  }
    
  // Check all the other characters are valid.
  for ( var i=1 ; i<sData.length ; i++ ) {
    Char = sData.charAt(i); 
    if (sValidChars.indexOf(Char) == -1) {
      return null;
    }
      
    // Only one decimal place allowed.
    if (Char == ".") {
      if (bDecimal) {
        return null;
      }
      bDecimal = true;
    }
  }
    
  return "num-html";
});