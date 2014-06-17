$(function() {

// global variables
var DEFAULT_NAMESPACE, NAMESPACE, NAMESPACES_READY, NAMESPACE_SHORTCUTS, FILE_CONTENTS, QUERY_RESULTS;
// LBS URLs do not currently work with non-HA and HA1 setups. Set this to true to use LBS URLs
if(false) {
   var RW_URL_PREFIX = '/bigdata/LBS/leader/', RO_URL_PREFIX = '/bigdata/LBS/read/';
} else {
   var RW_URL_PREFIX = '/bigdata/', RO_URL_PREFIX = '/bigdata/';
}
var CODEMIRROR_DEFAULTS, EDITORS = {}, ERROR_LINE_MARKERS = {}, ERROR_CHARACTER_MARKERS = {};
var PAGE_SIZE = 50, TOTAL_PAGES, CURRENT_PAGE;
var NAMESPACE_PARAMS = {
   'name': 'com.bigdata.rdf.sail.namespace',
   'index': 'com.bigdata.search.FullTextIndex.fieldsEnabled',
   'truthMaintenance': 'com.bigdata.rdf.sail.truthMaintenance',
   'quads': 'com.bigdata.rdf.store.AbstractTripleStore.quads'
};


CODEMIRROR_DEFAULTS = {
   lineNumbers: true,
   mode: 'sparql',
   extraKeys: {'Ctrl-,': moveTabLeft, 'Ctrl-.': moveTabRight}
};

// debug to access closure variables
$('html, textarea, select').bind('keydown', 'ctrl+d', function() { debugger; });

/* Modal functions */

function showModal(id) {
   $('#' + id).show();
   $('body').addClass('modal-open');
}

$('.modal-cancel').click(function() {
   $('body').removeClass('modal-open');
   $(this).parents('.modal').hide();
});

/* Search */

$('#search-form').submit(function(e) {
   e.preventDefault();
   var term = $(this).find('input').val();
   if(!term) {
      return;
   }
   var query = 'select ?s ?p ?o { ?o bds:search "' + term + '" . ?s ?p ?o . }'
   EDITORS.query.setValue(query);
   $('#query-errors').hide();
   $('#query-form').submit();
   showTab('query');
});

/* Tab selection */

$('#tab-selector a').click(function(e) {
   showTab($(this).data('target'));
});

function showTab(tab, nohash) {
   $('.tab').hide();
   $('#' + tab + '-tab').show();
   $('#tab-selector a').removeClass();
   $('a[data-target=' + tab + ']').addClass('active');
   if(!nohash && window.location.hash.substring(1).indexOf(tab) != 0) {
      window.location.hash = tab;
   }
   if(EDITORS[tab]) {
      EDITORS[tab].refresh();
   }
}

function moveTabLeft() {
   moveTab(false);
}

function moveTabRight() {
   moveTab(true);
}

function moveTab(next) {
   // get current position
   var current = $('#tab-selector .active');
   if(next) {
      if(current.next('a').length) {
         current.next().click();
      } else {
         $('#tab-selector a:first').click();
      }
   } else {
      if(current.prev().length) {
         current.prev().click();
      } else {
         $('#tab-selector a:last').click();
      }
   }
}

// these should be , and . but Hotkeys views those keypresses as these characters
$('html, textarea, select').bind('keydown', 'ctrl+¼', moveTabLeft);
$('html, textarea, select').bind('keydown', 'ctrl+¾', moveTabRight);

/* Namespaces */

function getNamespaces() {
   $.get(RO_URL_PREFIX + 'namespace?describe-each-named-graph=false', function(data) {
      $('#namespaces-list').empty();
      var rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
      var namespaces = namespaces = data.getElementsByTagNameNS(rdf, 'Description')
      for(var i=0; i<namespaces.length; i++) {
         var title = namespaces[i].getElementsByTagName('title')[0].textContent;
         var titleText = title == DEFAULT_NAMESPACE ? title + ' (default)' : title;
         var url = namespaces[i].getElementsByTagName('sparqlEndpoint')[0].getAttributeNS(rdf, 'resource');
         var use;
         if(title == NAMESPACE) {
            use = 'In use';
         } else {
            use = '<a href="#" class="use-namespace">Use</a>';
         }
         $('#namespaces-list').append('<li data-name="' + title + '">' + titleText + ' - ' + use + ' - <a href="#" class="delete-namespace">Delete</a> - <a href="#" class="namespace-properties">Properties</a> - <a href="' + RO_URL_PREFIX + 'namespace/' + title + '/sparql" class="namespace-service-description">Service Description</a></li>');
      }
      $('.use-namespace').click(function(e) {
         e.preventDefault();
         useNamespace($(this).parent().data('name'));
      });
      $('.delete-namespace').click(function(e) {
         e.preventDefault();
         deleteNamespace($(this).parent().data('name'));
      });
      $('.namespace-properties').click(function(e) {
         e.preventDefault();
         getNamespaceProperties($(this).parent().data('name'));
      });
      $('.namespace-properties-java').click(function(e) {
         e.preventDefault();
         getNamespaceProperties($(this).parent().data('name'), 'java');
      });
      $('.clone-namespace').click(function(e) {
         e.preventDefault();
         cloneNamespace($(this).parent().data('name'));
      });
      $('.namespace-service-description').click(function(e) {
         return confirm('This can be an expensive operation. Proceed anyway?');
      });
      NAMESPACES_READY = true;
   });
}

function selectNamespace(name) {
   // for programmatically selecting a namespace with just its name
   if(!NAMESPACES_READY) {
      setTimeout(function() { selectNamespace(name); }, 10);
   } else {
      $('#namespaces-list li[data-name=' + name + '] a.use-namespace').click();
   }
}

function useNamespace(name) {
   $('#current-namespace').html(name);
   NAMESPACE = name;
   getNamespaces();
}

function deleteNamespace(namespace) {
   // prevent default namespace from being deleted
   if(namespace == DEFAULT_NAMESPACE) {
      alert('You may not delete the default namespace.');
      return;
   }

   if(confirm('Are you sure you want to delete the namespace ' + namespace + '?')) {
      if(namespace == NAMESPACE) {
         // FIXME: what is the desired behaviour when deleting the current namespace?
      }
      var url = RW_URL_PREFIX + 'namespace/' + namespace;
      var settings = {
         type: 'DELETE',
         success: getNamespaces,
         error: function() { alert('Could not delete namespace ' + namespace); }
      };
      $.ajax(url, settings);
   }
}

function getNamespaceProperties(namespace, download) {
   var url = RO_URL_PREFIX + 'namespace/' + namespace + '/properties';
      if(!download) {
         $('#namespace-properties h1').html(namespace);
         $('#namespace-properties table').empty();
         $('#namespace-properties').show();
      }
   $.get(url, function(data) {
      var java = '';
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         if(download) {
            java += entry.getAttribute('key') + '=' + entry.textContent + '\n';
         } else {
            $('#namespace-properties table').append('<tr><td>' + entry.getAttribute('key') + '</td><td>' + entry.textContent + '</td></tr>');
         }
      });
      if(download) {
         downloadFile(java, 'text/x-java-properties', this.url.split('/')[3] + '.properties');
      }
   });
}

function cloneNamespace(namespace) {
   var url = RO_URL_PREFIX + 'namespace/' + namespace + '/properties';
   $.get(url, function(data) {
      var reversed_params = {};
      for(var key in NAMESPACE_PARAMS) {
         reversed_params[NAMESPACE_PARAMS[key]] = key;
      }
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         var key = entry.getAttribute('key');
         if(reversed_params[key] == 'name') {
            return;
         }
         if(key in reversed_params) {
            $('#new-namespace-' + reversed_params[key]).prop('checked', entry.textContent.trim() == 'true');
         }
      });
      $('#new-namespace-name').focus();
   });
}

function createNamespace(e) {
   e.preventDefault();
   // get new namespace name and config options
   var params = {};
   params.name = $('#new-namespace-name').val().trim();
   if(!params.name) {
      return;
   }
   params.index = $('#new-namespace-index').is(':checked');
   params.truthMaintenance = $('#new-namespace-truth-maintenance').is(':checked');
   params.quads = $('#new-namespace-quads').is(':checked');
   // TODO: validate namespace
   // TODO: allow for other options to be specified
   var data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n<properties>\n';
   for(key in NAMESPACE_PARAMS) {
      data += '<entry key="' + NAMESPACE_PARAMS[key] + '">' + params[key] + '</entry>\n';
   }
   data += '</properties>';
   var settings = {
      type: 'POST',
      data: data,
      contentType: 'application/xml',
      success: function() { $('#new-namespace-name').val(''); getNamespaces(); },
      error: function(jqXHR, textStatus, errorThrown) { debugger;alert(jqXHR.responseText); }
   };
   $.ajax(RW_URL_PREFIX + 'namespace', settings);
}
$('#namespace-create').submit(createNamespace);

function getDefaultNamespace() {
   $.get(RO_URL_PREFIX + 'namespace?describe-each-named-graph=false&describe-default-namespace=true', function(data) {
      // Chrome does not work with rdf\:Description, so look for Description too
      var defaultDataset = $(data).find('rdf\\:Description, Description');
      DEFAULT_NAMESPACE = defaultDataset.find('title')[0].textContent;
      var url = defaultDataset.find('sparqlEndpoint')[0].attributes['rdf:resource'].textContent;
      useNamespace(DEFAULT_NAMESPACE);
   });
}

getDefaultNamespace();


/* Namespace shortcuts */

NAMESPACE_SHORTCUTS = {
   'Bigdata': {
      'bd': 'http://www.bigdata.com/rdf#',
      'bds': 'http://www.bigdata.com/rdf/search#',
      'gas': 'http://www.bigdata.com/rdf/gas#',
      'hint': 'http://www.bigdata.com/queryHints#'
   },
   'W3C': {
      'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
      'owl': 'http://www.w3.org/2002/07/owl#',
      'skos': 'http://www.w3.org/2004/02/skos/core#',
      'xsd': 'http://www.w3.org/2001/XMLSchema#'
   },
   'Dublin Core': {
      'dc': 'http://purl.org/dc/elements/1.1/',
      'dcterm': 'http://purl.org/dc/terms/',
      'void': 'http://rdfs.org/ns/void#'
   },
   'Social/Other': {
      'foaf': 'http://xmlns.com/foaf/0.1/',
      'schema': 'http://schema.org/',
      'sioc': 'http://rdfs.org/sioc/ns#'
   }
};

$('.namespace-shortcuts').html('');
for(var category in NAMESPACE_SHORTCUTS) {
   var select = $('<select><option>' + category + '</option></select>').appendTo($('.namespace-shortcuts'));
   for(var ns in NAMESPACE_SHORTCUTS[category]) {
      select.append('<option value="' + NAMESPACE_SHORTCUTS[category][ns] + '">' + ns + '</option>');
   }
}

$('.namespace-shortcuts select').change(function() {
   var uri = this.value;
   var tab = $(this).parents('.tab').attr('id').split('-')[0];
   var current = EDITORS[tab].getValue();

   if(current.indexOf(uri) == -1) {
      var ns = $(this).find(':selected').text();
      EDITORS[tab].setValue('prefix ' + ns + ': <' + uri + '>\n' + current);
   }

   // reselect group label
   this.selectedIndex = 0;
});


/* Update */

function handleDragOver(e) {
   e.stopPropagation();
   e.preventDefault();
   e.originalEvent.dataTransfer.dropEffect = 'copy';
}

function handleFile(e) {
   e.stopPropagation();
   e.preventDefault();

   if(e.type == 'drop') {
      var files = e.originalEvent.dataTransfer.files;
   } else {
      var files = e.originalEvent.target.files;
   }
   
   // only one file supported
   if(files.length > 1) {
      alert('Ignoring all but first file');
   }
   
   var f = files[0];
   
   // if file is too large, tell user to supply local path
   if(f.size > 1048576 * 100) {
      alert('File too large, enter local path to file');
      $('#update-box').val('/path/to/' + f.name);
      setType('path');
      $('#update-box').prop('disabled', false)
      $('#large-file-message, #clear-file').hide();
   } else {
      var fr = new FileReader();
      fr.onload = function(e2) {
         if(f.size > 10240) {
            // do not use textarea
            $('#update-box').prop('disabled', true)
            $('#filename').html(f.name);
            $('#large-file-message, #clear-file').show()
            $('#update-box').val('');
            FILE_CONTENTS = e2.target.result;
         } else {
            // display file contents in the textarea
            clearFile();
            $('#update-box').val(e2.target.result);
         }
         guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
      };
      fr.readAsText(f);
   }

   $('#update-file').val('');
}

function clearFile(e) {
   if(e) {
      e.preventDefault();
   }
   $('#update-box').prop('disabled', false)
   $('#large-file-message, #clear-file').hide()
   FILE_CONTENTS = null;
}

function guessType(extension, content) {
   // try to guess type
   if(extension == 'rq') {
      // SPARQL
      setType('sparql');
   } else if(extension in rdf_types) {
      // RDF
      setType('rdf', rdf_types[extension]);
   } else {
      // extension is no help, see if we can find some SPARQL commands
      setType(identify(content));
   }
}

function identify(text, considerPath) {
   if(considerPath) {
      // match Unix, Windows or HTTP paths
      // file:// is optional for local paths
      // when file:// is not present, Windows paths may use \ or / and must include a :
      // when file:// is present, Windows paths must use / and may include a :
      // http[s]:// is mandatory for HTTP paths
      var unix = /^(file:\/\/)?((\/[^\/]+)+)$/;
      var windows = /^(((file:\/\/)([A-Za-z]:?([\/][^\/\\]+)+))|([A-Za-z]:([\\\/][^\\\/]+)+))$/;
      var http = /^https?:\/((\/[^\/]+)+)$/;
      if(unix.test(text.trim()) || windows.test(text.trim()) || http.test(text.trim())) {
         return 'path';
      }
   }
   
   text = text.toUpperCase();
   for(var i=0; i<sparql_update_commands.length; i++) {
      if(text.indexOf(sparql_update_commands[i]) != -1) {
         return 'sparql';
      }
   }

   return 'rdf';
}

function handlePaste(e) {   
   // if the input is currently empty, try to identify the pasted content
   var that = this;
   if(this.value == '') {
      setTimeout(function() { setType(identify(that.value, true)); }, 10);
   } 
}

function setType(type, format) {
   $('#update-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type').val(format);
   }
   setUpdateSettings(type);
}

$('#update-type').change(function() { setUpdateSettings(this.value); });
$('#rdf-type').change(function() { setUpdateMode('rdf'); });

function setUpdateSettings(type) {
   $('#rdf-type, label[for="rdf-type"]').attr('disabled', type != 'rdf');
   $('#update-tab .advanced-features input').attr('disabled', type != 'sparql');
   setUpdateMode(type);
}

function setUpdateMode(type) {
   var mode = '';
   if(type == 'sparql') {
      mode = 'sparql';
   } else if(type == 'rdf') {
      type = $('#rdf-type').val();
      if(type in rdf_modes) {
         mode = rdf_modes[type];
      }
   }
   EDITORS.update.setOption('mode', mode);
}

// .xml is used for both RDF and TriX, assume it's RDF
// We could check the parent element to see which it is
var rdf_types = {'nq': 'n-quads',
                 'nt': 'n-triples',
                 'n3': 'n3',
                 'rdf': 'rdf/xml',
                 'rdfs': 'rdf/xml',
                 'owl': 'rdf/xml',
                 'xml': 'rdf/xml',
                 'json': 'json',
                 'trig': 'trig',
                 'trix': 'trix',
                 //'xml': 'trix',
                 'ttl': 'turtle'};
                 
var rdf_content_types = {'n-quads': 'text/x-nquads',
                         'n-triples': 'text/plain',
                         'n3': 'text/rdf+n3',
                         'rdf/xml': 'application/rdf+xml',
                         'json': 'application/sparql-results+json',
                         'trig': 'application/x-trig',
                         'trix': 'application/trix',
                         'turtle': 'application/x-turtle'};

// key is value of RDF type selector, value is name of CodeMirror mode
var rdf_modes = {'n-triples': 'ntriples', 'rdf/xml': 'xml', 'json': 'json', 'turtle': 'turtle'};

var sparql_update_commands = ['INSERT', 'DELETE', 'LOAD', 'CLEAR'];

$('#update-file').change(handleFile);
$('#update-box').on('dragover', handleDragOver)
   .on('drop', handleFile)
   .on('paste', handlePaste)
   .on('input propertychange', function() { $('#update-errors').hide(); });
$('#clear-file').click(clearFile);

$('#update-update').click(submitUpdate);

EDITORS.update = CodeMirror.fromTextArea($('#update-box')[0], CODEMIRROR_DEFAULTS);
EDITORS.update.on('change', function() { 
   if(ERROR_LINE_MARKERS.update) {
      ERROR_LINE_MARKERS.update.clear();
      ERROR_CHARACTER_MARKERS.update.clear();
   }
});
EDITORS.update.addKeyMap({'Ctrl-Enter': submitUpdate});

function submitUpdate(e) {
   // Updates are submitted as a regular form for SPARQL updates in monitor mode, and via AJAX for non-monitor SPARQL, RDF & file path updates.
   // When submitted as a regular form, the output is sent to an iframe. This is to allow monitor mode to work.
   // jQuery only gives us data when the request is complete, so we wouldn't see monitor results as they come in.

   try {
      e.preventDefault();
   } catch(e) {}

   $('#update-response').show();

   var url = RW_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql';
   var settings = {
      type: 'POST',
      data: FILE_CONTENTS == null ? EDITORS.update.getValue() : FILE_CONTENTS,
      success: updateResponseXML,
      error: updateResponseError
   }

   // determine action based on type
   switch($('#update-type').val()) {
      case 'sparql':
         // see if monitor mode is on
         if($('#update-monitor').is(':checked')) {
            // create form and submit it, sending output to the iframe
            var form = $('<form id="update-monitor-form" method="POST" target="update-response-container">')
               .attr('action', url)
               .append($('<input name="update">').val(settings.data))
               .append('<input name="monitor" value="true">');
            if($('#update-analytic').is(':checked')) {
               form.append('<input name="analytic" value="true">')
            }
            form.appendTo($('body'));
            form.submit();
            $('#update-monitor-form').remove();
            $('#update-response iframe, #update-clear-container').show();
            $('#update-response pre').hide();
            return;
         }
         settings.data = 'update=' + encodeURIComponent(settings.data);
         if($('#update-analytic').is(':checked')) {
            settings.data += '&analytic=true';
         }
         settings.success = updateResponseHTML;
         break;
      case 'rdf':
         var type = $('#rdf-type').val();
         if(!type) {
            alert('Please select an RDF content type.');
            return;
         }
         settings.contentType = rdf_content_types[type];
         break;
      case 'path':
         // if no scheme is specified, assume a local path
         if(!/^(file|(https?)):\/\//.test(settings.data)) {
            settings.data = 'file://' + settings.data;
         }
         settings.data = 'uri=' + encodeURIComponent(settings.data);
         break;
   }

   $('#update-response pre').show().html('Running update...');   

   $.ajax(url, settings);
}

$('#update-clear').click(function() {
   $('#update-response, #update-clear-container').hide();
   $('#update-response pre').text('');
   $('#update-response iframe').attr('src', 'about:blank');
});

$('.advanced-features-toggle').click(function() {
   $(this).next('.advanced-features').toggle();
   return false;
});

function updateResponseHTML(data) {
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();
   $('#update-response pre').html(data);
}

function updateResponseXML(data) {
   var modified = data.childNodes[0].attributes['modified'].value;
   var milliseconds = data.childNodes[0].attributes['milliseconds'].value;
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();
   $('#update-response pre').text('Modified: ' + modified + '\nMilliseconds: ' + milliseconds);
}

function updateResponseError(jqXHR, textStatus, errorThrown) {
   $('#update-response, #update-clear-container').show();
   $('#update-response iframe').attr('src', 'about:blank').hide();
   $('#update-response pre').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   highlightError(jqXHR.statusText, 'update');
}


/* Query */

$('#query-box').on('input propertychange', function() { $('#query-errors').hide(); });
$('#query-form').submit(submitQuery);

$('#query-explain').change(function() {
   if(!this.checked) {
      $('#query-details').prop('checked', false);
   }
});

$('#query-details').change(function() {
   if(this.checked) {
      $('#query-explain').prop('checked', true);
   }
});

EDITORS.query = CodeMirror.fromTextArea($('#query-box')[0], CODEMIRROR_DEFAULTS);
EDITORS.query.on('change', function() { 
   if(ERROR_LINE_MARKERS.query) {
      ERROR_LINE_MARKERS.query.clear();
      ERROR_CHARACTER_MARKERS.query.clear();
   }
});
EDITORS.query.addKeyMap({'Ctrl-Enter': submitQuery});

function submitQuery(e) {
   try {
      e.preventDefault();
   } catch(e) {}

   // transfer CodeMirror content to textarea
   EDITORS.query.save();

   // do nothing if query is empty
   if($('#query-box').val().trim() == '') {
      return;
   }

   var url = RO_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql';
   var settings = {
      type: 'POST',
      data: $('#query-form').serialize(),
      headers: { 'Accept': 'application/sparql-results+json, application/rdf+xml' },
      success: showQueryResults,
      error: queryResultsError
   }

   $('#query-response').show().html('Query running...');
   $('#query-pagination').hide();

   $.ajax(url, settings);

   $('#query-explanation').empty();
   if($('#query-explain').is(':checked')) {
      settings = {
         type: 'POST',
         data: $('#query-form').serialize() + '&explain=' + ($('#query-details').is(':checked') ? 'details' : 'true'),
         dataType: 'html',
         success: showQueryExplanation,
         error: queryResultsError
      };
      $.ajax(url, settings);
   } else {
      $('#query-explanation').hide();
   }
}

$('#query-response-clear').click(function() {
   $('#query-response, #query-explanation').empty('');
   $('#query-response, #query-pagination, #query-explanation, #query-export-container').hide();
});

$('#query-export').click(function() { updateExportFileExtension(); showModal('query-export-modal'); });

var export_extensions = {
   "application/rdf+xml": ['RDF/XML', 'rdf', true],
   "application/x-turtle": ['N-Triples', 'nt', true],
   "application/x-turtle": ['Turtle', 'ttl', true],
   "text/rdf+n3": ['N3', 'n3', true],
   "application/trix": ['TriX', 'trix', true],
   "application/x-trig": ['TRIG', 'trig', true],
   "text/x-nquads": ['NQUADS', 'nq', true],

   "text/csv": ['CSV', 'csv', false, exportCSV],
   "application/sparql-results+json": ['JSON', 'json', false, exportJSON],
   // "text/tab-separated-values": ['TSV', 'tsv', false, exportTSV],
   "application/sparql-results+xml": ['XML', 'xml', false, exportXML]
};

for(var contentType in export_extensions) {
   var optgroup = export_extensions[contentType][2] ? '#rdf-formats' : '#non-rdf-formats';
   $(optgroup).append('<option value="' + contentType + '">' + export_extensions[contentType][0] + '</option>');
}

$('#export-format option:first').prop('selected', true);

$('#export-format').change(updateExportFileExtension);

function updateExportFileExtension() {
   $('#export-filename-extension').html(export_extensions[$('#export-format').val()][1]);
}

$('#query-download').click(function() {
   var dataType = $('#export-format').val();
   var filename = $('#export-filename').val();
   if(filename == '') {
      filename = 'export';
   }
   filename += '.' + export_extensions[dataType][1];
   if(export_extensions[dataType][2]) {
      // RDF
      var settings = {
         type: 'POST',
         data: JSON.stringify(QUERY_RESULTS),
         contentType: 'application/sparql-results+json',
         headers: { 'Accept': dataType },
         success: function() { downloadFile(data, dataType, filename); },
         error: downloadRDFError
      };
      $.ajax(RO_URL_PREFIX + 'sparql?workbench&convert', settings);
   } else {
      // not RDF
      export_extensions[dataType][3](filename);
   }
   $(this).siblings('.modal-cancel').click();
});

function downloadRDFError(jqXHR, textStatus, errorThrown) {
   alert(jqXHR.statusText);
}   

function exportXML(filename) {
   var xml = '<?xml version="1.0"?>\n<sparql xmlns="http://www.w3.org/2005/sparql-results#">\n\t<head>\n';
   var bindings = [];
   $('#query-response thead tr td').each(function(i, td) {
      xml += '\t\t<variable name="' + td.textContent + '"/>\n';
      bindings.push(td.textContent);
   });
   xml += '\t</head>\n\t<results>\n';
   $('#query-response tbody tr').each(function(i, tr) {
      xml += '\t\t<result>\n';
      $(tr).find('td').each(function(j, td) {
         var bindingType = td.className;
         if(bindingType == 'unbound') {
            return;
         }
         var dataType = $(td).data('datatype');
         if(dataType) {
            dataType = ' datatype="' + dataType + '"';
         } else {
            dataType = '';
         }
         var lang = $(td).data('lang');
         if(lang) {
            lang = ' xml:lang="' + lang + '"';
         } else {
            lang = '';
         }
         xml += '\t\t\t<binding name="' + bindings[j] + '"><' + bindingType + dataType + lang + '>' + td.textContent + '</' + bindingType + '></binding>\n';
      });
      xml += '\t\t</result>\n';
   });
   xml += '\t</results>\n</sparql>\n';
   downloadFile(xml, 'application/sparql-results+xml', filename);
}

function exportJSON(filename) {
   var json = JSON.stringify(QUERY_RESULTS);
   downloadFile(json, 'application/sparql-results+json', filename);
}

function exportCSV(filename) {
   var csv = '';
   $('#query-response table tr').each(function(i, tr) {
      $(tr).find('td').each(function(j, td) {
         if(j > 0) {
            csv += ',';
         }
         var val = td.textContent;
         // quote value if it contains " , \n or \r
         // replace " with ""
         if(val.match(/[",\n\r]/)) {
            val = '"' + val.replace('"', '""') + '"';
         }
         csv += val;
      });
      csv += '\n';
   });
   downloadFile(csv, 'text/csv', filename);
}

function downloadFile(data, type, filename) {
   var uri = 'data:' + type + ';charset=utf-8,' + encodeURIComponent(data);
   $('<a id="download-link" download="' + filename + '" href="' + uri + '">').appendTo('body')[0].click();
   $('#download-link').remove();
}

function showQueryResults(data) {
   $('#query-response').empty();
   $('#query-export-rdf').hide();
   $('#query-response, #query-pagination, #query-export-container').show();
   var table = $('<table>').appendTo($('#query-response'));
   if(this.dataTypes[1] == 'xml') {
      // RDF
      table.append($('<thead><tr><td>s</td><td>p</td><td>o</td></tr></thead>'));
      var rows = $(data).find('Description');
      for(var i=0; i<rows.length; i++) {
         // FIXME: are about and nodeID the only possible attributes here?
         var s = rows[i].attributes['rdf:about'];
         if(typeof(s) == 'undefined') {
            s = rows[i].attributes['rdf:nodeID'];
         }
         s = s.textContent;
         for(var j=0; j<rows[i].children.length; j++) {
            var p = rows[i].children[j].tagName;
            var o = rows[i].children[j].attributes['rdf:resource'];
            // FIXME: is this the correct behaviour?
            if(typeof(o) == 'undefined') {
               o = rows[i].children[j].textContent;
            } else {
               o = o.textContent;
            }
            var tr = $('<tr><td>' + (j == 0 ? s : '') + '</td><td>' + p + '</td><td>' + o + '</td>');
            table.append(tr);
         }
      }
   } else {
      // JSON
      // save data for export and pagination
      QUERY_RESULTS = data;

      if(typeof(data.boolean) != 'undefined') {
         // ASK query
         table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
         return;
      }

      // see if we have RDF data
      var isRDF = false;
      if(data.head.vars.length == 3 && data.head.vars[0] == 's' && data.head.vars[1] == 'p' && data.head.vars[2] == 'o') {
         isRDF = true;
      } else if(data.head.vars.length == 4 && data.head.vars[0] == 's' && data.head.vars[1] == 'p' && data.head.vars[2] == 'o' && data.head.vars[3] == 'c') {
         // see if c is used or not
         for(var i=0; i<data.results.bindings.length; i++) {
            if('c' in data.results.bindings[i]) {
               isRDF = false;
               break;
            }
         }

         if(isRDF) {
            // remove (unused) c variable from JSON
            data.head.vars.pop();
         }
      }

      if(isRDF) {
         $('#rdf-formats').prop('disabled', false);
      } else {
         $('#rdf-formats').prop('disabled', true);
         if($('#rdf-formats option:selected').length == 1) {
            $('#non-rdf-formats option:first').prop('selected', true);
         }
      }

      // put query variables in table header
      var thead = $('<thead>').appendTo(table);
      var tr = $('<tr>');
      for(var i=0; i<data.head.vars.length; i++) {
         tr.append('<th>' + data.head.vars[i] + '</th>');
      }
      thead.append(tr);
      table.append(thead);

      $('#total-results').html(data.results.bindings.length);
      setNumberOfPages();
      showPage(1);

      $('#query-response a').click(function(e) {
         e.preventDefault();
         explore(this.textContent);
      });
   }
}

function showQueryExplanation(data) {
   $('#query-explanation').html(data).show();
}

function queryResultsError(jqXHR, textStatus, errorThrown) {
   $('#query-response, #query-export-container').show();
   $('#query-response').text('Error! ' + textStatus + ' ' + jqXHR.responseText);
   highlightError(jqXHR.responseText, 'query');
}

function highlightError(description, pane) {
   var match = description.match(/line (\d+), column (\d+)/);
   if(match) {
      // highlight character at error position
      var line = match[1] - 1;
      var character = match[2] - 1;
      ERROR_LINE_MARKERS[pane] = EDITORS.query.doc.markText({line: line, ch: 0}, {line: line}, {className: 'error-line'});
      ERROR_CHARACTER_MARKERS[pane] = EDITORS.query.doc.markText({line: line, ch: character}, {line: line, ch: character + 1}, {className: 'error-character'});
   }
}

/* Pagination */

function setNumberOfPages() {
   TOTAL_PAGES = Math.ceil(QUERY_RESULTS.results.bindings.length / PAGE_SIZE);
   $('#result-pages').html(TOTAL_PAGES);
}

function setPageSize(n) {
   if(n == 'all') {
      n = QUERY_RESULTS.results.bindings.length;
   } else {
      n = parseInt(n, 10);
      if(typeof n != 'number' || n % 1 != 0 || n < 1 || n == PAGE_SIZE) {
         return;
      }
   }

   PAGE_SIZE = n;
   setNumberOfPages();
   // TODO: show page containing current first result
   showPage(1);
}

$('#results-per-page').change(function() { setPageSize(this.value); });
$('#previous-page').click(function() { showPage(CURRENT_PAGE - 1); });
$('#next-page').click(function() { showPage(CURRENT_PAGE + 1); });
$('#current-page').keyup(function(e) {
   if(e.which == 13) {
      var n = parseInt(this.value, 10);
      if(typeof n != 'number' || n % 1 != 0 || n < 1 || n > TOTAL_PAGES) {
         this.value = CURRENT_PAGE;
      } else {
         showPage(n);
      }
   }
});

function showPage(n) {
   if(typeof n != 'number' || n % 1 != 0 || n < 1 || n > TOTAL_PAGES) {
      return;
   }

   CURRENT_PAGE = n;

   // clear table results, leaving header
   $('#query-response tbody tr').remove();

   // work out indices for this page
   var start = (CURRENT_PAGE - 1) * PAGE_SIZE;
   var end = Math.min(CURRENT_PAGE * PAGE_SIZE, QUERY_RESULTS.results.bindings.length);

   // add matching bindings
   var table = $('#query-response table');
   for(var i=start; i<end; i++) {
         var tr = $('<tr>');
         for(var j=0; j<QUERY_RESULTS.head.vars.length; j++) {
            if(QUERY_RESULTS.head.vars[j] in QUERY_RESULTS.results.bindings[i]) {
               var binding = QUERY_RESULTS.results.bindings[i][QUERY_RESULTS.head.vars[j]];
               if(binding.type == 'sid') {
                  var text = getSID(binding);
               } else {
                  var text = binding.value;
                  if(binding.type == 'uri') {
                     text = abbreviate(text);
                  }
               }
               linkText = escapeHTML(text).replace(/\n/g, '<br>');
               if(binding.type == 'typed-literal') {
                  var tdData = ' class="literal" data-datatype="' + binding.datatype + '"';
               } else {
                  if(binding.type == 'uri' || binding.type == 'sid') {
                     text = '<a href="' + buildExploreHash(text) + '">' + linkText + '</a>';
                  }
                  var tdData = ' class="' + binding.type + '"';
                  if(binding['xml:lang']) {
                     tdData += ' data-lang="' + binding['xml:lang'] + '"';
                  }
               }
               tr.append('<td' + tdData + '>' + text + '</td>');
            } else {
               // no binding
               tr.append('<td class="unbound">');
            }
         }
         table.append(tr);
   }

   // update current results numbers
   $('#current-results').html((start + 1) + '-' + end);
   $('#current-page').val(n);
}

/* Explore */

$('#explore-form').submit(function(e) {
   e.preventDefault();
   var uri = $(this).find('input[type="text"]').val().trim();
   if(uri) {
      // add < > if they're not present and this is not a namespaced URI
      if(uri[0] != '<' && uri.match(/^\w+:\//)) {
         uri = '<' + uri;
         if(uri.slice(-1) != '>') {
            uri += '>';
         }
         $(this).find('input[type="text"]').val(uri);
      }
      loadURI(uri);

      // if this is a SID, make the components clickable
      // var re = /<< *(<[^<>]*>) *(<[^<>]*>) *(<[^<>]*>) *>>/;
      var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
      var match = uri.match(re);
      if(match) {
         var header = $('<h1>');
         header.append('<< <br>');
         for(var i=1; i<4; i++) {
            header.append($('<a href="' + buildExploreHash(match[i]) + '">').text(match[i])).append('<br>');
         }
         header.append(' >>');
         $('#explore-header').html(header);
      } else {
         $('#explore-header').html($('<h1>').text(uri));
      }
   }
});

function buildExploreHash(uri) {
   return '#explore:' + NAMESPACE + ':' + uri;
}

function loadURI(target) {
   // identify if this is a vertex or a SID
   target = target.trim().replace(/\n/g, ' ');
   // var re = /<< *(?:<[^<>]*> *){3} *>>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var vertex = !target.match(re);

   var vertexQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star) {\n\
  bind (URI as ?explore ) .\n\
  {\n\
    bind (<<?explore ?col1 ?col2>> as ?sid) . \n\
    bind (false as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  } union {\n\
    bind (<<?col1 ?col2 ?explore>> as ?sid) .\n\
    bind (true as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  }\n\
}\n\
group by ?col1 ?col2 ?incoming';

   var edgeQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star)\n\
with {\n\
  select ?explore where {\n\
    bind (SID as ?explore) .\n\
  }\n\
} as %_explore\n\
where {\n\
  include %_explore .\n\
  {\n\
    bind (<<?explore ?col1 ?col2>> as ?sid) . \n\
    bind (false as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  } union {\n\
    bind (<<?col1 ?col2 ?explore>> as ?sid) .\n\
    bind (true as ?incoming) . \n\
    optional {\n\
      { ?sid ?sidP ?star } union { ?star ?sidP ?sid }\n\
    }\n\
  }\n\
}\n\
group by ?col1 ?col2 ?incoming';

   if(vertex) {
      var query = vertexQuery.replace('URI', target);
   } else {
      var query = edgeQuery.replace('SID', target);
   }
   var settings = {
      type: 'POST',
      data: 'query=' + encodeURIComponent(query),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: updateExploreStart,
      error: updateExploreError
   };
   $.ajax(RO_URL_PREFIX + 'namespace/' + NAMESPACE + '/sparql', settings); 
}

function updateExploreStart(data) {
   var results = data.results.bindings.length > 0;

   // clear tables
   $('#explore-incoming, #explore-outgoing, #explore-attributes').html('<table>');
   $('#explore-results, #explore-results .box').show();

   // go through each binding, adding it to the appropriate table
   $.each(data.results.bindings, function(i, binding) {
      var cols = [binding.col1, binding.col2].map(function(col) {
         if(col.type == 'sid') {
            var uri = getSID(col);
         } else {
            var uri = col.value;
            if(col.type == 'uri') {
               uri = '<' + uri + '>';
            }
         }
         output = escapeHTML(uri).replace(/\n/g, '<br>');
         if(col.type == 'uri' || col.type == 'sid') {
            output = '<a href="' + buildExploreHash(uri) + '">' + output + '</a>';
         }
         return output;
      });
      var star = parseInt(binding.star.value);
      if(star > 0) {
         if(binding.incoming.value == 'true') {
            var sid = '<< <' +  binding.col1.value + '> <' + binding.col2.value + '> ' + $('#explore-form input[type=text]').val() + ' >>';
         } else {
            var sid = '<< ' + $('#explore-form input[type=text]').val() + ' <' +  binding.col1.value + '> <' + binding.col2.value + '> >>';
         }
         star = '<a href="' + buildExploreHash(sid) + '"><< * (' + star + ') >></a>';
      } else {
         star = '';
      }
      var row = '<tr><td>' + cols[0] + '</td><td>' + cols[1] + '</td><td>' + star + '</td></tr>';
      if(binding.incoming.value == 'true') {
         $('#explore-incoming table').append(row);
      } else {
         // either attribute or outgoing
         if(binding.col2.type == 'uri') {
            // outgoing
            $('#explore-outgoing table').append(row);
         } else {
            // attribute
            $('#explore-attributes table').append(row);
         }
      }
   });

   var sections = {incoming: 'Incoming Links', outgoing: 'Outgoing Links', attributes: 'Attributes'};
   for(var k in sections) {
      var id = '#explore-' + k;
      if($(id + ' table tr').length == 0) {
         $(id).html('No ' + sections[k]);
      } else {
         $(id).prepend('<h1>' + sections[k] + '</h1>');
      }
   }

   $('#explore-results a').click(function(e) {
      e.preventDefault();
      var components = parseHash(this.hash);
      exploreNamespacedURI(components[2], components[3]);
   });
}

function exploreNamespacedURI(namespace, uri, nopush) {
   if(!NAMESPACES_READY) {
      setTimeout(function() { exploreNamespacedURI(namespace, uri, nopush); }, 10);
   } else {
      selectNamespace(namespace);
      explore(uri, nopush);
   }
}

function explore(uri, nopush) {
   $('#explore-form input[type=text]').val(uri);
   $('#explore-form').submit();
   showTab('explore', true);
   if(!nopush) {
      history.pushState(null, null, '#explore:' + NAMESPACE + ':' + uri);
   }
}

function parseHash(hash) {
   // match #tab:namespace:uri
   // :namespace:uri group optional
   // namespace optional
   var re = /#([^:]+)(?::([^:]*):(.+))?/;
   return hash.match(re);
}

// handle history buttons and initial display of first tab
window.addEventListener("popstate", handlePopState);
$(handlePopState);

function handlePopState() {
   var hash = parseHash(this.location.hash);
   if(!hash) {
      $('#tab-selector a:first').click();
   } else {
      if(hash[1] == 'explore') {
         exploreNamespacedURI(hash[2], hash[3], true);
      } else {
         $('a[data-target=' + hash[1] + ']').click();
      }
   }
}

function updateExploreError(jqXHR, textStatus, errorThrown) {
   $('#explore-results .box').html('').hide();
   $('#explore-header').text('Error! ' + textStatus + ' ' + jqXHR.statusText);
   $('#explore-results, #explore-header').show();
}

/* Status */

$('#tab-selector a[data-target=status]').click(getStatus);

function getStatus(e) {
   if(e) {
      e.preventDefault();
   }
   $.get(RO_URL_PREFIX + 'status', function(data) {
      // get data inside a jQuery object
      data = $('<div>').append(data);
      getStatusNumbers(data);
   });
}

function getStatusNumbers(data) {
   $('#status-text').html(data);
   $('#status-text a').eq(1).click(function(e) { e.preventDefault(); showQueries(false); return false; });
   $('#status-text a').eq(2).click(function(e) { e.preventDefault(); showQueries(true); return false; });
}

$('#show-queries').click(function(e) {
   e.preventDefault();
   showQueries(false);
});

$('#show-query-details').click(function(e) {
   e.preventDefault();
   showQueries(true);
});

function showQueries(details) {
   var url = RO_URL_PREFIX + 'status?showQueries';
   if(details) {
      url += '=details';
   }
   $.get(url, function(data) {
      // get data inside a jQuery object
      data = $('<div>').append(data);

      // update status numbers
      getStatusNumbers(data);

      // clear current list
      $('#running-queries').empty();

      data.find('h1').each(function(i, e) {
         e = $(e);
         // get numbers string, which includes cancel link
         var form = e.next();
         // HA mode has h1 before running queries
         if(form[0].tagName != 'FORM') {
            return;
         }
         var numbers = form.find('p')[0].textContent;
         // remove cancel link
         numbers = numbers.substring(0, numbers.lastIndexOf(','));
         // get query id
         var queryId = form.find('input[type=hidden]').val();
         // get SPARQL
         var sparqlContainer = form.next().next();
         var sparql = sparqlContainer.html();

         if(details) {
            var queryDetails = $('<div>').append(sparqlContainer.nextUntil('h1')).html();
         } else {
            var queryDetails = '<a href="#">Details</a>';
         }

         // got all data, create a li for each query
         var li = $('<li><div class="query"><pre>' + sparql + '</pre></div><div class="query-numbers">' + numbers + ', <a href="#" class="cancel-query">Cancel</a></div><div class="query-details">' + queryDetails + '</div>');
         li.find('a').data('queryId', queryId);
         $('#running-queries').append(li);
      });

      $('.cancel-query').click(cancelQuery);
      $('.query-details a').click(getQueryDetails);
   });
}

function cancelQuery(e) {
   e.preventDefault();
   if(confirm('Cancel query?')) {
      var id = $(this).data('queryId');
      $.post(RW_URL_PREFIX + 'status?cancelQuery&queryId=' + id, function() { getStatus(); });
      $(this).parents('li').remove();
   }
}

function getQueryDetails(e) {
   e.preventDefault();
   var id = $(this).data('queryId');
   $.ajax({url: RO_URL_PREFIX + 'status?showQueries=details&queryId=' + id,
      success: function(data) {
         // get data inside a jQuery object
         data = $('<div>').append(data);

         // update status numbers
         getStatusNumbers(data);

         // details begin after second pre
         var details = $('<div>').append($(data.find('pre')[1]).nextAll()).html();

         $(this).parent().html(details);
      },
      context: this
   });
}


/* Performance */

$('#tab-selector a[data-target=performance]').click(function(e) {
   $.get(RO_URL_PREFIX + 'counters', function(data) {
      $('#performance-tab .box').html(data);
   });
});

/* Utility functions */

function getSID(binding) {
   return '<<\n ' + abbreviate(binding.value['s'].value) + '\n ' + abbreviate(binding.value['p'].value) + '\n ' + abbreviate(binding.value['o'].value) + '\n>>';
}

function abbreviate(uri) {
   for(var ns in NAMESPACE_SHORTCUTS) {
      if(uri.indexOf(NAMESPACE_SHORTCUTS[ns]) == 0) {
         return uri.replace(NAMESPACE_SHORTCUTS[ns], ns + ':');
      }
   }
   return '<' + uri + '>';
}

function unabbreviate(uri) {
   if(uri.charAt(0) == '<') {
      // not abbreviated
      return uri;
   }
   // get namespace
   var namespace = uri.split(':', 1)[0];
   return '<' + uri.replace(namespace, NAMESPACE_SHORTCUTS[namespace]) + '>';
}

function parseSID(sid) {
   // var re = /<< <([^<>]*)> <([^<>]*)> <([^<>]*)> >>/;
   var re = /<< *([^ ]+) +([^ ]+) +([^ ]+) *>>/;
   var matches = sid.match(re);
   return {'s': matches[1], 'p': matches[2], 'o': matches[3]};
}

function escapeHTML(text) {
   return $('<div/>').text(text).html();
}

});
