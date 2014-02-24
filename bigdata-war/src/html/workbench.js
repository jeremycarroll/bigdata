$(function() {

/* Tab selection */

$('#tab-selector a').click(function(e) {
   showTab($(this).data('target'));
});

if(window.location.hash) {
   showTab(window.location.hash.substr(1));
} else {
   $('#tab-selector a:first').click();
}

function showTab(tab) {
   $('.tab').hide();
   $('#' + tab + '-tab').show();
   $('#tab-selector a').removeClass();
   $('a[data-target=' + tab + ']').addClass('active');
   window.location.hash = tab;
}

/* Namespaces */

function getNamespaces() {
   $.get('/namespace', function(data) {
      var namespaces = data.getElementsByTagName('Namespace');
      for(var i=0; i<namespaces.length; i++) {
         var namespace = namespaces[i].textContent;
      }
   });
}

// default namespace
namespace = 'kb';
$('.namespace').val(namespace);

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
   if(f.size > 100 * 1048576) {
      alert('File too large, enter local path to file');
      $('#load-box').val('/path/to/' + f.name);
      setType('path');
      $('#load-file').val('');
      $('#large-file-message').hide();
      return;
   }
   
   // if file is small enough, populate the textarea with it
   if(f.size < 10 * 1024) {
      holder = '#load-box';
      $('#load-hidden').val('');
      $('#large-file-message').hide();
   } else {
      // store file contents in hidden input and clear textarea
      holder = '#load-hidden';
      $('#load-box').val('');
      $('#large-file-message').show();
   }
   var fr = new FileReader();
   fr.onload = function(e2) {
      $(holder).val(e2.target.result);
      guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
   };
   fr.readAsText(f);
   $('#load-file').val('');
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
   text = text.toUpperCase();

   if(considerPath) {
      // match Unix or Windows paths
      var re = /^(((\/[^\/]+)+)|([A-Z]:([\\\/][^\\\/]+)+))$/;
      if(re.test(text.trim())) {
         return 'path';
      }
   }
   
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
// stores the id of the element that contains the data to be sent
var holder = '#load-box';

$('#load-file').change(handleFile);
$('#load-box').on('dragover', handleDragOver);
$('#load-box').on('drop', handleFile);
$('#load-box').on('paste', handlePaste);
$('#load-type').change(handleTypeChange);

$('#load-load').click(function() {
   // determine action based on type
   var settings = {
      type: 'POST',
      data: $(holder).val(),
      success: updateResponseXML,
      error: updateResponseError
   }
   switch($('#load-type').val()) {
      case 'sparql':
         settings.data = 'update=' + encodeURI(settings.data);
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
         settings.data = 'uri=file://' + encodeURI(settings.data);
         break;
   }

   $.ajax('/sparql', settings); 
});

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

$('#query-form').submit(function() {
   var settings = {
      type: 'POST',
      data: $(this).serialize(),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: showQueryResults,
      error: queryResultsError
   }
   $.ajax('/sparql', settings);
   return false;
});

$('#query-response-clear').click(function() {
   $('#query-response').html('');   
});

function showQueryResults(data) {
   $('#query-response').html('');
   var table = $('<table>').appendTo($('#query-response'));
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
         tr.append('<td>' + data.results.bindings[i][vars[j]].value + '</td>');
      }
      table.append(tr);
   }

}

function queryResultsError(jqXHR, textStatus, errorThrown) {
   $('#query-response').text('Error! ' + textStatus + ' ' + errorThrown);
}


/* Navigator */

$('#navigator').submit(function() {
   // get URI
   var uri = $('#navigator-uri').val();
   if(uri) {
      loadURI(uri);   
   }
   return false;
});

function loadURI(uri) {
   // send query to server
   var query = 'select * \
                  where { \
                     bind (<URI> as ?vertex) . \
                     { \
                        bind (<<?vertex ?p ?o>> as ?sid) . \
                        optional \
                        { \
                           { \
                              ?sid ?sidP ?sidO . \
                           } union { \
                              ?sidS ?sidP ?sid . \
                           } \
                        } \
                     } union { \
                        bind (<<?s ?p ?vertex>> as ?sid) . \
                        optional \
                        { \
                           { \
                              ?sid ?sidP ?sidO . \
                           } union { \
                              ?sidS ?sidP ?sid . \
                           } \
                        } \
                     } \
                  }';
   
   query = query.replace('URI', uri);
   var settings = {
      type: 'POST',
      data: 'query=' + encodeURI(query),
      dataType: 'json',
      accepts: {'json': 'application/sparql-results+json'},
      success: updateNavigationStart,
      error: updateNavigationError
   };
   $.ajax('/sparql', settings); 
}

function updateNavigationStart(data) {
   var disp = $('#navigator-display');
   disp.html('');
   // see if we got any results
   if(data.results.bindings.length == 0) {
      disp.append('No vertex found!');
      return;
   }
   
   var vertex = data.results.bindings[0].vertex;
   disp.append('<h3>' + vertex.value + '</h3>');
   var outbound=[], inbound=[], attributes=[];
   for(var i=0; i<data.results.bindings.length; i++) {
      var binding = data.results.bindings[i];
      // TODO: are attributes always on outbound relationships?
      if('o' in binding) {
         if(binding.o.type == 'uri') {
            outbound.push(binding);
         } else {
            attributes.push(binding);
         }      
      } else {
         inbound.push(binding);
      }
   }
   
   if(outbound.length) {
      disp.append('<h4>Outbound links</h4>');
      var table = $('<table>').appendTo(disp);
      for(var i=0; i<outbound.length; i++) {
         var linkAttributes = outbound[i].sidP.value + ': ' + outbound[i].sidO.value;  
         table.append('<tr><td>' + outbound[i].p.value + '</td><td><a href="#">' + outbound[i].o.value + '</a></td><td>' + linkAttributes + '</td></tr>');
      }
   }

   if(inbound.length) {
      disp.append('<h4>Inbound links</h4>');
      var table = $('<table>').appendTo(disp);
      for(var i=0; i<inbound.length; i++) {
         var linkAttributes = inbound[i].sidP.value + ': ' + inbound[i].sidO.value;  
         table.append('<tr><td><a href="#">' + inbound[i].s.value + '</a></td><td>' + inbound[i].p.value + '</td><td>' + linkAttributes + '</td></tr>');
      }
   }

   if(attributes.length) {
      disp.append('<h4>Attributes</h4>');
      var table = $('<table>').appendTo(disp);
      for(var i=0; i<attributes.length; i++) {
         table.append('<tr><td>' + attributes[i].p.value + '</td><td>' + attributes[i].o.value + '</td></tr>');
      }
   }
   
   disp.find('a').click(function() { loadURI(this.text); return false; });
}

function updateNavigationError(jqXHR, textStatus, errorThrown) {
   $('#navigator-display').html('Error! ' + textStatus + ' ' + errorThrown);
}

});
