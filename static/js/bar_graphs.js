var debug1, debug2;
        $(function(){
            var bambooUrl = 'http://localhost:8080/',
                observationsUrl = bambooUrl + 'datasets',
                loadPage = function(datasetURL) { 
                    $.post(observationsUrl, { url: datasetURL}, function(bambooIdDict) {
                        var $tabsUl = $('#tabs'),
                            $contentDiv = $('#content'),
                            $groupingSelect = $('#grouping-select'),
                            $histogramSelect = $('#histogram-select'),
                            $datasourceChangeButton = $('#datasource-change-button'),
                            $datasourceUrl = $('#datasource-url');
                            
                            makeTitle = function (slug) {
                                var word, i, words = slug.split('_');
                                for(i = 0; i < words.length; i++) {
                                  word = words[i];
                                  words[i] = word.charAt(0).toUpperCase() + word.slice(1);
                                }
                                return words.join(' ');
                            },
                            jsonUrlFromIDAndGroup = function(id, group) {
                                if(group) {
                                    return bambooUrl + 'calculate?id=' + id + '&group=' + group;
                                } else {
                                    return bambooUrl + 'calculate?id=' + id;
                                }
                            },
                            refreshPage = function() {
                                makeGraphs(bambooIdDict["id"], $groupingSelect.val());
                            },
                            makeGraphs = function(id, group) {
                                $.getJSON(jsonUrlFromIDAndGroup(id, group), function (datasets) {
                                    /* CLEAR THE PAGE FIRST */
                                    _([$contentDiv, $tabsUl, $groupingSelect]).each(function(x) {x.empty();});
                                    _([$groupingSelect]).each(function(x) { x.unbind('change');});

                                    /* SET UP THE CONTROLS FOR THIS PAGE */
                                    /* Populate the grouping select with possible grouping options; only once per datasets (at the (ALL) key) */    
                                    _(datasets["(ALL)"]).chain()
                                        .map(function(x) { return '<option value="' + x.name + '">' + makeTitle(x.name) + '</option>'; })
                                        .each(function(x) { $groupingSelect.append(x); });
                                    if(group) {
                                        $groupingSelect.val(group);
                                    } else {
                                        $groupingSelect.prepend('<option value=""> </option>');
                                        $groupingSelect.val("");
                                    }
                                    $groupingSelect.change(function() {
                                        makeGraphs(id, $(this).val());
                                    });
                                    
                                    /* Populate the histogram selector */
                                    console.log($histogramSelect.children());
                                    if($histogramSelect.children().length==0) { /* TODO: refactor */
                                        _(datasets["(ALL)"]).chain()
                                            .map(function(x) { return '<option value="' + x.name + '">' + makeTitle(x.name) + '</option>'; })
                                            .each(function(x) { $histogramSelect.append(x); });
                                        $histogramSelect.change(function() {
                                            refreshPage(); 
                                        });
                                    }; 
                                    
                                    /* Enable the change button to work */
                                    $datasourceChangeButton.click(function() {
                                        loadPage($datasourceUrl.val());
                                    });
                                    

                                    _.each(datasets, function(dataset, key) { 
                                        var $tabPane,
                                            $textSummaries,
                                            $graphSummaries,
                                            dataElement,
                                            data,
                                            dataSize,
                                            dataKey,
                                            keyValSeparated,
                                            $thisDiv;

                                        /* One-time actions really; move out of loop ? */
                                        if(key==="(ALL)") {
                                       
                                            /* TODO: HACK because anchor tags and () don't play together. */
                                            key = "ALL";
                                        }

                                        /* MAKE THE TAB PANE */
                                        $('<li />').html(
                                              $('<a />', {
                                                  text: key,
                                                  'data-toggle': 'tab',
                                                  href: '#' + key
                                              })
                                            ).appendTo($tabsUl);
                                        $tabPane = $('<div />')
                                                      .attr('id', key)
                                                      .addClass('tab-pane')
                                                      .appendTo($contentDiv);
                                        $graphSummaries = $('<div />')
                                                      .attr('id', 'graph-summaries')
                                                      .appendTo($tabPane);
                                        $textSummaries = $('<div />')
                                                      .attr('id', 'text-summaries')
                                                      .appendTo($tabPane);

                                        for (dataKey in dataset) {
                                            dataElement = dataset[dataKey];
                                            if(_($histogramSelect.val()).find(function(x) {return (x === dataElement.name);})) { /* TODO: refactor */
                                                    /* TODO replace using the form schema */
                                                    dataElement.titleName = makeTitle(dataElement.name);

                                                    $thisDiv = $('<div />')
                                                                  .addClass('gg')
                                                                  .data('target', key)
                                                                  .appendTo($('<div />', {style:'float:left'}).appendTo($graphSummaries));

                                                    data = dataElement.data;
                                                    dataSize = _.size(data);
                                                    if(dataSize == 0 || dataElement.name.charAt(0)=='_') {
                                                        continue;
                                                    /*} else if (dataSize == 1) {
                                                
                                                      $textSummaries.append("All (" + _.values(data)[0] + ") responses for <br/>`" + dataElement.titleName + "`: <br/>" + _.keys(data)[0] + "<br/><br/>"); */
                                                    } else { 
                                                        keyValSeparated = {'x': _.keys(data), 'y': _.values(data)};
                                                        if (typeof (keyValSeparated.y[0]) === "number") {
                                                            /* if number make pure histogram */
                                                            gg.graph(keyValSeparated)
                                                                  .layer(gg.layer.bar()
                                                                  .map('x','x').map('y','y'))
                                                                  .opts({'width': Math.min(dataSize*80 + 100, 400), 'height':'200',
                                                                         'padding-right':'90', 'title':dataElement.titleName,
                                                                         'legend-postion':'bottom'})
                                                                  .render($thisDiv.get(0));
                                                        } 
                                                    }
                                            }
                                        };
                            });
                                    $('#tabs a:last').tab('show');
                                });
                            };
                        makeGraphs(bambooIdDict['id']);
                    }, 'json');
                },
                sampleDataSetUrl = 'http://formhub.org/education/forms/schooling_status_format_18Nov11/data.csv';
                loadPage(sampleDataSetUrl);
                
        });

