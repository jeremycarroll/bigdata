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
      $('#mp-box').val('/path/to/' + f.name);
      setType('path');
      $('#mp-file').val('');
      $('#large-file-message').hide();
      return;
   }
   
   // if file is small enough, populate the textarea with it
   if(f.size < 10 * 1024) {
      holder = '#mp-box';
      $('#mp-hidden').val('');
      $('#large-file-message').hide();
   } else {
      // store file contents in hidden input and clear textarea
      holder = '#mp-hidden';
      $('#mp-box').val('');
      $('#large-file-message').show();
   }
   var fr = new FileReader();
   fr.onload = function(e2) {
      $(holder).val(e2.target.result);
      guessType(f.name.split('.').pop().toLowerCase(), e2.target.result);
   };
   fr.readAsText(f);
   $('#mp-file').val('');
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
   $('#rdf-type').toggle($(this).val() == 'rdf');
}

function setType(type, format) {
   $('#mp-type').val(type);
   if(type == 'rdf') {
      $('#rdf-type').show();
      $('#rdf-type').val(format);
   } else {
      $('#rdf-type').hide();
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
var holder = '#mp-box';

$('#mp-file').change(handleFile);
$('#mp-box').on('dragover', handleDragOver);
$('#mp-box').on('drop', handleFile);
$('#mp-box').on('paste', handlePaste);
$('#mp-type').change(handleTypeChange);

$('#mp-send').click(function() {
   // determine action based on type
   var settings = {
      type: 'POST',
      data: $(holder).val(),
      success: updateResponseXML,
      error: updateResponseError
   }
   switch($('#mp-type').val()) {
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

function updateResponseHTML(data, textStatus, jqXHR) {
   $('#response').html(data);
}

function updateResponseXML(data, textStatus, jqXHR) {
   var modified = data.childNodes[0].attributes['modified'].value;
   var milliseconds = data.childNodes[0].attributes['milliseconds'].value;
   $('#response').text('Modified: ' + modified + '\nMilliseconds: ' + milliseconds);
}

function updateResponseError(jqXHR, textStatus, errorThrown) {
   $('#response').text('Error! ' + textStatus + ' ' + errorThrown);
}
