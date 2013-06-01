{{ xTicks }}

var x_axis = new Rickshaw.Graph.Axis.X( {
    graph: graph,
    orientation: 'bottom',
    ticksTreatment: 'glow',
    {{ ticks }}
    element: document.getElementById('x_axis')
} );
