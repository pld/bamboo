var bambooUrl = 'http://localhost:8080/';
var observationsUrl = bambooUrl + 'datasets';

/* UTILS */
function makeTitle(slug) {
    var word, i, words = slug.split('_');
    for(i = 0; i < words.length; i++) {
      word = words[i];
      words[i] = word.charAt(0).toUpperCase() + word.slice(1);
    }
    return words.join(' ');
};

/* PAGE BUILDING */
var $tabsUl = $('#tabs'),
    $altTabsUl = $('#alt-tabs'),
    $sideBySide = $('#side-by-side'),
    $contentDiv = $('#content'),
    $groupingSelect = $('#grouping-select'),
    $histogramSelect = $('#histogram-select'),
    $datasourceUrl = $('#datasource-url');
function clearPage() {
    _([$contentDiv, $tabsUl, $altTabsUl, $groupingSelect, $histogramSelect]).each(function(x) {x.empty();});
    _([$groupingSelect, $histogramSelect, $sideBySide]).each(function(x) { x.unbind('change');}); 
}
function makePageShell(groups, currentGroup) {
    var groups2Select = function(groups, $select) {
            _(groups).chain()
                .map(function(x) { return '<option value="' + x + '">' + makeTitle(x) + '</option>'; })
                .each(function(x) { $select.append(x); });
    };

    /* Populate the histogram selector */
    groups2Select(groups, $histogramSelect);
    $histogramSelect.change(function() {
        $.each($histogramSelect.children(), function(i, groupOption) { $('#' + (groupOption.value) + '.gg').hide(); });
        $.each($histogramSelect.val(), function(i, group) { $('#' + group + '.gg').show(); });
    });

    /* Populate the grouping select with possible grouping options; only once per datasets (at the (ALL) key) */    
    groups.unshift(""); /* unshift = prepend */
    groups2Select(groups, $groupingSelect);
    $groupingSelect.val(currentGroup);
}
         
/* GLUE LOGIC */
function jsonUrlFromIDAndGroup(id, group) {
    return bambooUrl + 'calculate?id=' + id + (group ? ('&group=' + group) : '');
}

/* GRAPH BUILDING */
function makeNavAndContainerForGroup(groupKey) {
    /* TAB NAV */
    $('<li />').html(
          $('<a />', {
              text: groupKey,
              'data-toggle': 'tab',
              href: '#' + groupKey
          })
        ).appendTo($tabsUl);
    /* CONTAINER WHERE GRAPH CONTENT WILL SIT */
    $('<div />')
        .attr('id', groupKey)
        .addClass('tab-pane group-nav')
        .appendTo($contentDiv);
}

function makeAltNavAndContainerForGroup(groupKey) {
    $('<div />').html(
          $('<a />', {
              text: groupKey,
              href: '#' + groupKey
          })
        ).attr('id', groupKey)
        .addClass('span3 group-nav')
        .appendTo($altTabsUl);
}
function makeInternalContainerForGroup(groupKey) {
    var $tabPane = $('#' + groupKey + '.group-nav');
    return $('<div />')
        .addClass('gg')
        .data('target', groupKey)
        .appendTo($('<div />', {style:'float:left'}).appendTo($tabPane));
}
function renderDataSet(dataset, groupKey) {
    var dataElement,
        data,
        dataSize,
        dataKey,
        keyValSeparated,
        $thisDiv;
    for (dataKey in dataset) {
        dataElement = dataset[dataKey];
        dataElement.titleName = makeTitle(dataElement.name);

        $thisDiv = makeInternalContainerForGroup(groupKey);
        $thisDiv.attr('id', dataElement.name)

        data = dataElement.data;
        dataSize = _.size(data);
        if(dataSize == 0 || dataElement.name.charAt(0)=='_') {
            continue;
        } else { 
            keyValSeparated = {'x': _.keys(data), 'y': _.values(data)};
            if (typeof (keyValSeparated.y[0]) === "number") {
                /* if number make pure histogram */
                gg.graph(keyValSeparated)
                      .layer(gg.layer.bar()
                      .map('x','x').map('y','y'))
                      .opts({'width': Math.min(dataSize*60 + 100, 300), 'height':'200',
                             'padding-right':'50', 'title':dataElement.titleName, 'title-size':12,
                             'legend-postion':'bottom'})
                      .render($thisDiv.get(0));
            } 
        }
    };
}

function loadPage(datasetURL) { 
    $.post(observationsUrl, { url: datasetURL}, function(bambooIdDict) {
        var makeGraphs = function(id, group) {
            $.getJSON(jsonUrlFromIDAndGroup(id, group), function (datasets) {
                /* DEAL WITH (ALL) */
                datasets["ALL"] = datasets["(ALL)"];
                delete datasets["(ALL)"];

                /* CLEAR THE PAGE FIRST */
                clearPage();

                /* SET UP THE CONTROLS FOR THIS PAGE */
                makePageShell(_(datasets['ALL']).pluck('name'), group);
                $groupingSelect.change(function() { /* TODO : can refactor into makePageShell somehow? */
                    makeGraphs(id, $(this).val());
                });
                $sideBySide.change(function() {
                    makeGraphs(id, $groupingSelect.val());
                });
                
                if($('#side-by-side:checked').length) {    
                    var count = 3; 
                    _.each(datasets, function(dataset, groupKey) {
                        if (count) {
                            makeAltNavAndContainerForGroup(groupKey);
                            renderDataSet(dataset, groupKey);
                            count--;
                        }
                    });
                } else {
                    _.each(datasets, function(dataset, groupKey) { 
                        makeNavAndContainerForGroup(groupKey);
                        renderDataSet(dataset, groupKey);
                    });
                }

                $('#tabs a:last').tab('show');
            });
        };
        makeGraphs(bambooIdDict['id']);
    }, 'json');
};
$(function(){
    sampleDataSetUrl = 'http://formhub.org/education/forms/schooling_status_format_18Nov11/data.csv';
    loadPage(sampleDataSetUrl);
        
    /* And make the datasource change button change the whole page */
    $('#datasource-change-button').click(function() {
        loadPage($datasourceUrl.val());
    });
});

