$(function() {

/* Search */

$('#search-form').submit(function(e) {
   e.preventDefault();
   var term = $(this).find('input').val();
   if(!term) {
      return;
   }
   var query = 'select ?s ?p ?o { ?o bds:search "' + term + '" . ?s ?p ?o . }'
   $('#query-box').val(query);
   $('#query-form').submit();
   showTab('query');
});

/* Tab selection */

$('#tab-selector a').click(function(e) {
   showTab($(this).data('target'));
});

function showTab(tab) {
   $('.tab').hide();
   $('#' + tab + '-tab').show();
   $('#tab-selector a').removeClass();
   $('a[data-target=' + tab + ']').addClass('active');
   window.location.hash = tab;
}

function moveTab(next) {
   // get current position
   var current = $('#tab-selector .active');
   if(next) {
      if(current.next().length) {
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
$('html, textarea, select').bind('keydown', 'ctrl+¼', function() { moveTab(false); });
$('html, textarea, select').bind('keydown', 'ctrl+¾', function() { moveTab(true); });

/* Namespaces */

function getNamespaces() {
   $.get('/bigdata/namespace?describe-each-named-graph=false', function(data) {
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
         $('#namespaces-list').append('<li data-name="' + title + '" data-url="' + url + '">' + titleText + ' - ' + use + ' - <a href="#" class="delete-namespace">Delete</a> - <a href="#" class="namespace-properties">Properties</a> - <a href="/bigdata/namespace/' + title + '/sparql" class="namespace-service-description">Service Description</a></li>');
      }
      $('.use-namespace').click(function(e) {
         e.preventDefault();
         useNamespace($(this).parent().data('name'), $(this).parent().data('url'));
      });
      $('.delete-namespace').click(function(e) {
         e.preventDefault();
         deleteNamespace($(this).parent().data('name'));
      });
      $('.namespace-properties').click(function(e) {
         e.preventDefault();
         getNamespaceProperties($(this).parent().data('name'));
      });
      $('.namespace-service-description').click(function(e) {
         return confirm('This can be an expensive operation. Proceed anyway?');
      });
   });
}

function useNamespace(name, url) {
   $('#current-namespace').html(name);
   NAMESPACE = name;
   NAMESPACE_URL = url;
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
      var url = '/bigdata/namespace/' + namespace;
      var settings = {
         type: 'DELETE',
         success: getNamespaces,
         error: function() { alert('Could not delete namespace ' + namespace); }
      };
      $.ajax(url, settings);
   }
}

function getNamespaceProperties(namespace) {
   $('#namespace-properties h1').html(namespace);
   $('#namespace-properties table').empty();
   $('#namespace-properties').show();
   var url = '/bigdata/namespace/' + namespace + '/properties';
   $.get(url, function(data) {
      $.each(data.getElementsByTagName('entry'), function(i, entry) {
         $('#namespace-properties table').append('<tr><td>' + entry.getAttribute('key') + '</td><td>' + entry.textContent + '</td></tr>');
      });
   });
}

function createNamespace(e) {
   e.preventDefault();
   var input = $(this).find('input[type=text]');
   var namespace = input.val();
   if(!namespace) {
      return;
   }
   // TODO: validate namespace
   // TODO: allow for other options to be specified
   var data = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n<properties>\n<entry key="com.bigdata.rdf.sail.namespace">' + namespace + '</entry>\n</properties>';
   var settings = {
      type: 'POST',
      data: data,
      contentType: 'application/xml',
      success: function() { input.val(''); getNamespaces(); },
      error: function(jqXHR, textStatus, errorThrown) { alert(errorThrown); }
   };
   $.ajax('/bigdata/namespace', settings);
}
$('#namespace-create').submit(createNamespace);

function getDefaultNamespace() {
   $.get('/bigdata/namespace?describe-each-named-graph=false&describe-default-namespace=true', function(data) {
      // Chrome does not work with rdf\:Description, so look for Description too
      var defaultDataset = $(data).find('rdf\\:Description, Description');
      DEFAULT_NAMESPACE = defaultDataset.find('title')[0].textContent;
      var url = defaultDataset.find('sparqlEndpoint')[0].attributes['rdf:resource'].textContent;
      useNamespace(DEFAULT_NAMESPACE, url);
   });
}
var DEFAULT_NAMESPACE, NAMESPACE, NAMESPACE_URL, fileContents;

getDefaultNamespace();


/* Namespace shortcuts */

$('.namespace-shortcuts li').click(function() {
   var textarea = $(this).parents('.tab').find('textarea');
   var current = textarea.val();
   var ns = $(this).data('ns');

   if(current.indexOf(ns) == -1) {
      textarea.val(ns + '\n' + current);
   }
});


/* Load */

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
      $('#load-box').val('/path/to/' + f.name);
      setType('path');
      $('#load-box').prop('disabled', false)
      $('#large-file-message, #clear-file').hide();
   } else {
      var fr = new FileReader();
      fr.onload = function(e2) {
         if(f.size > 10240) {
            // do not use textarea
            $('#load-box').prop('disabled', true)
            $('#large-file-message, #clear-file').show()
            $('#load-box').val('');
            fileContents = e2.target.result;
         } else {
            // display file contents in the textarea
            clearFile();
            $('#load-box').val(e2.target.result);
         }
         guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
      };
      fr.readAsText(f);
   }

   $('#load-file').val('');
}

function clearFile(e) {
   if(e) {
      e.preventDefault();
   }
   $('#load-box').prop('disabled', false)
   $('#large-file-message, #clear-file').hide()
   fileContents = null;
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
      var windows = /^((file:\/\/)([A-Za-z]:?([\/][^\/\\]+)+))|([A-Za-z]:([\\\/][^\\\/]+)+)$/;
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

function handleTypeChange(e) {
   $('#rdf-type-container').toggle($(this).val() == 'rdf');
}

function setType(type, format) {
   $('#load-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type-container').show();
      $('#rdf-type').val(format);
   } else {
      $('#rdf-type-container').hide();
   }
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
                 'trig': 'trig',
                 'trix': 'trix',
                 //'xml': 'trix',
                 'ttl': 'turtle'};
                 
var rdf_content_types = {'n-quads': 'application/n-quads',
                         'n-triples': 'text/plain',
                         'n3': 'text/n3',
                         'rdf/xml': 'application/rdf+xml',
                         'trig': 'application/trig',
                         'trix': 'application/trix',
                         'turtle': 'text/turtle'};

var sparql_update_commands = ['INSERT', 'DELETE'];

$('#load-file').change(handleFile);
$('#load-box').on('dragover', handleDragOver)
   .on('drop', handleFile)
   .on('paste', handlePaste)
   .bind('keydown', 'ctrl+return', submitLoad)
   .change(handleTypeChange);
$('#clear-file').click(clearFile);

$('#load-load').click(submitLoad);

function submitLoad(e) {
   e.preventDefault();

   var settings = {
      type: 'POST',
      data: fileContents == null ? $('#load-box').val() : fileContents,
      success: updateResponseXML,
      error: updateResponseError
   }

   // determine action based on type
   switch($('#load-type').val()) {
      case 'sparql':
         settings.data = 'update=' + encodeURIComponent(settings.data);
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

   $.ajax(NAMESPACE_URL, settings); 
}

$('#load-clear').click(function() {
   $('#load-response').text('');
});

$('#advanced-features-toggle').click(function() {
   $('#advanced-features').toggle();
   return false;
});

function updateResponseHTML(data) {
   $('#load-response').html(data);
}

function updateResponseXML(data) {
   var modified = data.childNodes[0].attributes['modified'].value;
   var milliseconds = data.childNodes[0].attributes['milliseconds'].value;
   $('#load-response').text('Modified: ' + modified + '\nMilliseconds: ' + milliseconds);
}

function updateResponseError(jqXHR, textStatus, errorThrown) {
   $('#load-response').text('Error! ' + textStatus + ' ' + errorThrown);
}


/* Query */

$('#query-box').bind('keydown', 'ctrl+return', function(e) { e.preventDefault(); $('#query-form').submit(); });
$('#query-form').submit(submitQuery);

function submitQuery(e) {
   e.preventDefault();

   var settings = {
      type: 'POST',
      data: $(this).serialize(),
      headers: { 'Accept': 'application/sparql-results+json, application/rdf+xml' },
      success: showQueryResults,
      error: queryResultsError
   }

   $.ajax(NAMESPACE_URL, settings);

   $('#query-explanation').empty();
   if($('#query-explain').is(':checked')) {
      settings = {
         type: 'POST',
         data: $(this).serialize() + '&explain=details',
         dataType: 'html',
         success: showQueryExplanation,
         error: queryResultsError
      };
      $.ajax(NAMESPACE_URL, settings);
   } else {
      $('#query-explanation').hide();
   }
}

$('#query-response-clear').click(function() {
   $('#query-response, #query-explanation').empty('');
   $('#query-explanation').hide();
});

$('#query-export-csv').click(exportCSV);
$('#query-export-json').click(exportJSON);
$('#query-export-xml').click(exportXML);

function exportXML() {
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
   downloadFile(xml, 'application/sparql-results+xml', 'export.xml');
}

function exportJSON() {
   var json = {}
   if($('#query-response table').hasClass('boolean')) {
      json.head = {};
      json['boolean'] = $('#query-response td').text();
   } else {
      json.head = {vars: []};
      $('#query-response thead tr td').each(function(i, td) {
         json.head.vars.push(td.textContent);
      });
      json.bindings = [];
      $('#query-response tbody tr').each(function(i, tr) {
         var binding = {};
         $(tr).find('td').each(function(j, td) {
            var bindingFields = {}
            var bindingType = td.className;
            if(bindingType == 'unbound') {
               return;
            }
            bindingFields.type = bindingType;
            var dataType = $(td).data('datatype');
            if(dataType) {
               bindingFields.type = dataType;
            }
            var lang = $(td).data('lang');
            if(lang) {
               bindingFields.lang = lang;
            }
            bindingFields.value = td.textContent;
            binding[json.head.vars[j]] = bindingFields;
         });
         json.bindings.push(binding);
      });
   }
   json = JSON.stringify(json);
   downloadFile(json, 'application/sparql-results+json', 'export.json');
}

function exportCSV() {
   // FIXME: escape commas
   var csv = '';
   $('#query-response table tr').each(function(i, tr) {
      $(tr).find('td').each(function(j, td) {
         if(j > 0) {
            csv += ',';
         }
         csv += td.textContent;
      });
      csv += '\n';
   });
   downloadFile(csv, 'application/csv', 'export.csv');
}

function downloadFile(data, type, filename) {
   var uri = 'data:' + type + ';charset=utf-8,' + encodeURIComponent(data);
   $('<a id="download-link" download="' + filename + '" href="' + uri + '">').appendTo('body')[0].click();
   $('#download-link').remove();
}

function showQueryResults(data) {
   $('#query-response').empty();
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
      if(typeof(data.boolean) != 'undefined') {
         // ASK query
         table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
         return;
      }
      var thead = $('<thead>').appendTo(table);
      var vars = [];
      var tr = $('<tr>');
      for(var i=0; i<data.head.vars.length; i++) {
         tr.append('<td>' + data.head.vars[i] + '</td>');
         vars.push(data.head.vars[i]);
      }
      thead.append(tr);
      table.append(thead);
      for(var i=0; i<data.results.bindings.length; i++) {
         var tr = $('<tr>');
         for(var j=0; j<vars.length; j++) {
            if(vars[j] in data.results.bindings[i]) {
               var binding = data.results.bindings[i][vars[j]];
               if(binding.type == 'sid') {
                  var text = getSID(binding);
               } else {
                  var text = binding.value;
               }
               text = escapeHTML(text);
               text = text.replace(/\n/g, '<br>');
               if(binding.type == 'typed-literal') {
                  var tdData = ' class="literal" data-datatype="' + binding.datatype + '"';
               } else {
                  if(binding.type == 'uri' || binding.type == 'sid') {
                     text = '<a href="#">' + text + '</a>';
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
   $('#query-response').text('Error! ' + textStatus + ' ' + errorThrown);
}

/* Explore */

$('#explore-form').submit(function(e) {
   e.preventDefault();
   var uri = $(this).find('input').val();
   if(uri) {
      loadURI(uri);

      // if this is a SID, make the components clickable
      var re = /<< *<([^<>]*)> *<([^<>]*)> *<([^<>]*)> *>>/;
      var match = uri.match(re);
      if(match) {
         $('#explore-header').html('<h1>&lt;&lt; &lt;<a href="#">' + match[1] + '</a>&gt;<br>&lt;<a href="#">' + match[2] + '</a> &gt;<br>&lt;<a href="#">' + match[3] + '</a> &gt; &gt;&gt;</h1>');
         $('#explore-header h1 a').click(function(e) {
            e.preventDefault();
            explore(this.text);
         });
      } else {
         $('#explore-header').html('<h1>' + uri + '</h1>');
      }
   }
});

function loadURI(target) {
   // identify if this is a vertex or a SID
   target = target.trim().replace(/\n/g, ' ');
   var re = /<< *(?:<[^<>]*> *){3} *>>/;
   var vertex = !target.match(re);

   var vertexQuery = '\
select ?col1 ?col2 ?incoming (count(?star) as ?star) {\n\
  bind (<URI> as ?explore ) .\n\
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
   console.log('Explore query for ' + (vertex ? 'vertex ' : 'edge ') + target);
   console.log(query);
   var settings = {
      type: 'POST',
      data: 'query=' + encodeURI(query),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: updateExploreStart,
      error: updateExploreError
   };
   $.ajax(NAMESPACE_URL, settings); 
}

function updateExploreStart(data) {
   console.log('Explore results');
   console.log(data);
   var results = data.results.bindings.length > 0;

   // clear tables
   $('#explore-incoming, #explore-outgoing, #explore-attributes').html('<table>');

   // go through each binding, adding it to the appropriate table
   $.each(data.results.bindings, function(i, binding) {
      var cols = [binding.col1, binding.col2].map(function(col) {
         if(col.type == 'sid') {
            var output = getSID(col);
         } else {
            var output = col.value;
         }
         output = escapeHTML(output).replace(/\n/g, '<br>');
         if(col.type == 'uri' || col.type == 'sid') {
            output = '<a href="#">' + output + '</a>';
         }
         return output;
      });
      var star = parseInt(binding.star.value);
      if(star > 0) {
         if(binding.incoming.value == 'true') {
            var sid = '<< <' +  binding.col1.value + '> <' + binding.col2.value + '> <' + $('#explore-form input[type=text]').val() + '> >>';
         } else {
            var sid = '<< <' + $('#explore-form input[type=text]').val() + '> <' +  binding.col1.value + '> <' + binding.col2.value + '> >>';
         }
         star = '<a href="#" data-sid="' + sid + '"><< * (' + star + ') >></a>';
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
      explore($(this).data('sid') ? $(this).data('sid') : this.text);
   });
}

function explore(uri) {
   $('#explore-form input[type=text]').val(uri);
   $('#explore-form').submit();
   showTab('explore');
}

function updateExploreError(jqXHR, textStatus, errorThrown) {
   $('#explore-results .box').html('');
   $('#explore-header').html('Error! ' + textStatus + ' ' + errorThrown);
}

/* Status */

$('#tab-selector a[data-target=status]').click(function(e) {
   $.get('/bigdata/status', function(data) {
      var accepted = data.match(/Accepted query count=(\d+)/)[1];
      var running = data.match(/Running query count=(\d+)/)[1];
      var numbers = $(data).get(-1).textContent;
      $('#accepted-query-count').html(accepted);
      $('#running-query-count').html(running);
      $('#status-numbers').html(numbers);
   });
});

$('#show-queries').click(function(e) {
   e.preventDefault();
   $.get('/bigdata/status?showQueries', function(data) {
      // clear current list
      $('#running-queries').empty();

      // get data inside a jQuery object
      data = $('<div>').append(data);
      data.find('h1').each(function(i, e) {
         // per running query, data is structured h1 form (with numbers/cancel data) h2 pre (with SPARQL)
         e = $(e);
         // get numbers string, which includes cancel link
         var form = e.next();
         var numbers = form.find('p')[0].textContent;
         // remove cancel link
         numbers = numbers.substring(0, numbers.lastIndexOf(','));
         // get query id
         var queryId = form.find('input[type=hidden]').val();
         // get SPARQL
         var sparql = form.next().next().html();

         // got all data, create a li for each query
         var li = $('<li><div class="query"><pre>' + sparql + '</pre></div><div class="query-numbers">' + numbers + ', <a href="#" class="cancel-query">Cancel</a></div><div class="query-details"><a href="#" class="query-details collapsed">Details</a></div>');
         li.find('a').data('queryId', queryId);
         $('#running-queries').append(li);
      });

      $('.cancel-query').click(cancelQuery);
      $('a.query-details').click(getQueryDetails);
   });
});

function cancelQuery(e) {
   e.preventDefault();
   if(confirm('Cancel query?')) {
      var id = $(this).data('queryId');
      $.post('/bigdata/?cancel&queryId=' + id);
      $(this).parents('li').remove();
   }
}

function getQueryDetails(e) {}

/* Performance */

$('#tab-selector a[data-target=performance]').click(function(e) {
   $.get('/bigdata/counters', function(data) {
      $('#performance-tab .box').html(data);
   });
});

/* Utility functions */

function getSID(binding) {
   return '<< <' + binding.value['sid-s'].value + '>\n<' + binding.value['sid-p'].value + '>\n<' + binding.value['sid-o'].value + '> >>';
}

function parseSID(sid) {
   var re = /<< <([^<>]*)> <([^<>]*)> <([^<>]*)> >>/;
   var matches = sid.match(re);
   return {'s': matches[1], 'p': matches[2], 'o': matches[3]};
}

function escapeHTML(text) {
   return $('<div/>').text(text).html();
}

if(window.location.hash) {
   $('a[data-target=' + window.location.hash.substring(1) + ']').click();
} else {
   $('#tab-selector a:first').click();
}

});
