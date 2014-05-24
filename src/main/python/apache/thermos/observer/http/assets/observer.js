/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var Task = new Class({
   data: {},

   initialize: function(taskId, taskType) {
     this.taskType = taskType
     this.taskId  = taskId
     this.visible  = false
     this.setElement()
   },

   // task[active.task_id.processes.waiting]
   taskStr: function(components) {
     return 'task[' + ([this.taskType, this.taskId].append(components).join()) + ']'
   },

   transitionElement: function(dom_id, newval) {
     if (!$(dom_id)) {
       // alert('could not find dom_id' + dom_id)
       return
     }

     curval = $(dom_id).innerHTML

     // handle specialcases
     newval = this.translateElement(dom_id, newval)

     if (newval != curval) {
       $(dom_id).innerHTML = newval
       var morphElement = new Fx.Morph(dom_id, {
           duration: 'long',
           transition: Fx.Transitions.Sine.easeOut
       });
       morphElement.start({ 'color': ["#00FF00", "#222222"]})
     }
   },

   applyUpdate: function(update) {
     this.data = update
     this.updateElement()
   },

   /*
     TODO(wickman)  Instead do the 2-3 necessary transition functions, then make
     a schema that maps attributes to the appropriate transition function.
   */
   translateElement: function(dom_id, value) {
     if (dom_id === this.taskStr(['processes', 'running']) ||
         dom_id === this.taskStr(['processes', 'waiting']) ||
         dom_id === this.taskStr(['processes', 'success']) ||
         dom_id === this.taskStr(['processes', 'failed'])) {
       return value.length
     }

     if (dom_id == this.taskStr(['resource_consumption', 'cpu'])) {
       if (value) {
         return value.toFixed(2)
       }
     }

     return value
   },

   /*
     TODO(wickman)  Ditto to above.
   */
   updateElement: function() {
     for (prefix_attr in this.data) {
       if (this.data.hasOwnProperty(prefix_attr)) {
         if (prefix_attr == "task_id") {
           this.transitionElement(this.taskStr([prefix_attr]), this.translateUid(this.data[prefix_attr]))
         } else if (instanceOf(this.data[prefix_attr], String) || instanceOf(this.data[prefix_attr], Number)) {
           this.transitionElement(this.taskStr([prefix_attr]), this.data[prefix_attr])
         } else if (instanceOf(this.data[prefix_attr], Object)) {
           for (suffix_attr in this.data[prefix_attr])
             if (this.data[prefix_attr].hasOwnProperty(suffix_attr))
               this.transitionElement(this.taskStr([prefix_attr, suffix_attr]),
                                      this.data[prefix_attr][suffix_attr])
         }
       }
     }
   },

   translateUid: function(taskId) {
     return "<a href='/task/" + taskId + "'>" + taskId + "</a>"
   },

   setElement: function() {
     this.element = new Element('tr', {
      'id': 'task[' + this.taskType + '][' + this.taskId + ']'}).adopt(
         new Element('td', { 'id': this.taskStr(['task_id']), 'html': this.translateUid(this.taskId)}),
         new Element('td', { 'id': this.taskStr(['name']) }),
         new Element('td', { 'id': this.taskStr(['resource_consumption', 'cpu']) }),
         new Element('td', { 'id': this.taskStr(['resource_consumption', 'ram']) }),
         new Element('td', { 'id': this.taskStr(['resource_consumption', 'disk']) }),
         new Element('td', { 'id': this.taskStr(['processes', 'waiting']) }),
         new Element('td', { 'id': this.taskStr(['processes', 'running']) }),
         new Element('td', { 'id': this.taskStr(['processes', 'success']) }),
         new Element('td', { 'id': this.taskStr(['processes', 'failed']) }))
   }
})

var TableManager = new Class({
  activeTasks: {},
  visibleChildren: [],

  initialize: function(tableType) {
    this.tableType = tableType
    this.setElement()
    this.getChildren()
    this.startPolling()
  },

  setElement: function() {
    this.element = new Element('table', { 'id': 'table[' + this.tableType + ']', 'class': 'common-table zebra-striped', 'cellpadding': '0' })
    this.element.adopt(
      new Element('thead').
        adopt(new Element('tr', { 'id': 'task[' + this.tableType + '][superheader]', 'class': 'meta-headers' })
          .adopt(new Element('th', { 'html': "", 'colspan': 2 }),
                 new Element('th', { 'html': "consumed", 'colspan': 3 }),
                 new Element('th', { 'html': "processes", 'colspan': 4 })
                )
              )
        .adopt(new Element('tr', { 'id': 'task[' + this.tableType + '][header]' })
          .adopt(new Element('th', { 'id': 'task[' + this.tableType + '][header][task_id]',   'html': "task_id" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][name]',      'html': "name" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][resource_consumption][cpu]', 'html': "cpu" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][resource_consumption][ram]', 'html': "ram" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][resource_consumption][disk]','html': "disk" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][processes][waiting]', 'html': "waiting" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][processes][running]', 'html': "running"}),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][processes][success]', 'html': "success" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][processes][failed]',  'html': "failed" })
                )
              )
            )
    this.tbody = new Element('tbody', { 'id': this.tableType + '_tbody' })
    this.element.adopt(this.tbody)
  },

  toElement: function() {
    return this.element
  },

  startPolling: function() {
    this.taskIdPoller = setInterval(this.tableType + 'TableManager.getChildren();', 2500)
    this.dataPoller = setInterval(this.tableType + 'TableManager.refreshVisibleChildren();', 5300)
  },

  getChildren: function() {
    new Request.JSON({
      'url': '/j/task_ids/' + this.tableType + '/-20',
      'method': 'get',
      'onComplete': function(response) {
        if (response) {
          var newChildren = Array.from(response.task_ids)
          this.visibleChildren = newChildren

          // first set all children to invisible
          for (taskId in this.activeTasks) {
            this.activeTasks[taskId].visible = false
          }

          // set new children visible
          for (var k = 0; k < newChildren.length; k++) {
            if (!(newChildren[k] in this.activeTasks)) {
              this.activeTasks[newChildren[k]] = new Task(newChildren[k], this.tableType)
            }
            this.activeTasks[newChildren[k]].visible = true
          }

          // clear then adopt them
          while (this.tbody.childNodes.length > 0)
            this.tbody.removeChild(this.tbody.firstChild);
          for (var k = 0; k < newChildren.length; k++)
            this.tbody.adopt(this.activeTasks[newChildren[k]].element)
        } else {
          clearInterval(this.taskIdPoller)
        }
      }.bind(this)
    }).send()
  },

  refreshVisibleChildren: function() {
     // first get visible children
     new Request.JSON({
       'url': '/j/task',
       'method': 'get',
       'data': { 'task_id': this.visibleChildren.join() },
       'onComplete': function(response) {
         if (response) {
           if (!response) return;
           for (taskId in response) {
             if (response.hasOwnProperty(taskId))
               this.activeTasks[taskId].applyUpdate(response[taskId])
           }
         } else {
           clearInterval(this.dataPoller)
         }
       }.bind(this)
     }).send()
   },
})
