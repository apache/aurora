var Task = new Class({
   data: {},
   /*
     from observer.py
     data: {
       #     {uid}: {
       #        uid: int,
       #        job: { name: string, X owner: string, X group: string }
       #        name: string,
       #        replica: int,
       #        state: string [ACTIVE, SUCCESS, FAILED]
       #     X  ports: { name1: 'url', name2: 'url2' }
       #        resource_consumption: {cpu: , ram: , disk:}
       #     X  timeline: {
       #           axes: [ 'time', 'cpu', 'ram', 'disk'],
       #           data: [ (1299534274324, 0.9, 1212492188, 493932999392),
       #                   (1299534275327, 0.92, 23432423423, 52353252343), ... ]
       #        }
       #        processes: {     -> names only
       #           waiting: [],
       #           running: [],
       #           success: [],
       #           failed:  []
       #        }
       #     }
    */

   initialize: function(uid, taskType) {
     this.taskType = taskType
     this.uid      = uid
     this.visible  = false
     this.setElement()
   },

   // task[active.uid.processes.waiting]
   taskStr: function(components) {
     return 'task[' + ([this.taskType, this.uid].append(components).join()) + ']'
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

   translateElement: function(dom_id, value) {
     // should really use a RE here.
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

   updateElement: function() {
     for (prefix_attr in this.data) { if (this.data.hasOwnProperty(prefix_attr)) {
       // special case the "processes" prefix
       if (instanceOf(this.data[prefix_attr], String) || instanceOf(this.data[prefix_attr], Number)) {
         this.transitionElement(this.taskStr([prefix_attr]), this.data[prefix_attr])
       } else if (instanceOf(this.data[prefix_attr], Object)) {
         for (suffix_attr in this.data[prefix_attr])
           if (this.data[prefix_attr].hasOwnProperty(suffix_attr))
             this.transitionElement(this.taskStr([prefix_attr, suffix_attr]),
                                    this.data[prefix_attr][suffix_attr])
       }
     }}
   },

   setElement: function() {
     this.element = new Element('tr', {
      'id': 'task[' + this.taskType + '][' + this.uid + ']'}).adopt(
         new Element('td', { 'id': this.taskStr(['uid']), 'html': this.uid }),
         new Element('td', { 'id': this.taskStr(['job', 'name']) }),
         new Element('td', { 'id': this.taskStr(['name']) }),
         new Element('td', { 'id': this.taskStr(['replica']) }),
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
    $('defaultLayout').adopt(this.element)

    this.getChildren()
    this.startPolling()
  },

  setElement: function() {
    this.element = new Element('table', { 'id': 'table[' + this.tableType + ']', 'class': 'common-table zebra-striped', 'cellpadding': '0' })
    this.element.adopt(
      new Element('thead').
        adopt(new Element('tr', { 'id': 'task[' + this.tableType + '][superheader]', 'class': 'meta-headers' })
          .adopt(new Element('th', { 'html': "", 'colspan': 4 }),
                 new Element('th', { 'html': "consumed", 'colspan': 3 }),
                 new Element('th', { 'html': "processes", 'colspan': 4 })
                )
              )
        .adopt(new Element('tr', { 'id': 'task[' + this.tableType + '][header]' })
          .adopt(new Element('th', { 'id': 'task[' + this.tableType + '][header][uid]',       'html': "uid" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][job][name]', 'html': "job" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][name]',      'html': "task" }),
                 new Element('th', { 'id': 'task[' + this.tableType + '][header][replica]',   'html': "replica" }),
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
    this.uidPoller = setInterval(this.tableType + 'TableManager.getChildren();', 2500)
    this.dataPoller = setInterval(this.tableType + 'TableManager.refreshVisibleChildren();', 5300)
  },

  getChildren: function() {
    new Request.JSON({
      'url': '/uids',
      'method': 'post',
      'data': { 'type': this.tableType,
                'offset': -20 },
      'onComplete': function(response) {
        if (response) {
          var newChildren = Array.from(response.data)
          this.visibleChildren = newChildren

          // first set all children to invisible
          for (uid in this.activeTasks) {
            this.activeTasks[uid].visible = false
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
          clearInterval(this.uidPoller)
        }
      }.bind(this)
    }).send()
  },

  refreshVisibleChildren: function() {
     // first get visible children
     new Request.JSON({
       'url': '/task',
       'method': 'post',
       'data': { 'uid': this.visibleChildren.join() },
       'onComplete': function(response) {
         if (response) {
           if (!response.data) return;
           for (uid in response.data) {
             if (response.data.hasOwnProperty(uid))
               this.activeTasks[uid].applyUpdate(response.data[uid])
           }
         } else {
           clearInterval(this.dataPoller)
         }
       }.bind(this)
     }).send()
   },
})

activesTableManager = ''
window.addEvent("domready", function() {
    activeTableManager = new TableManager('active')
  finishedTableManager = new TableManager('finished')
})
