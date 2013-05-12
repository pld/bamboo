var x_axis = new Rickshaw.Graph.Axis.X( {
    graph: graph,
    orientation: 'bottom',
    {{ ticks }}
    element: document.getElementById('x_axis')
} );
