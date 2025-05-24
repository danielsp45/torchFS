#import "@preview/plotst:0.2.0": *

#let graphtest = {
  // Plot 1:
// The data to be displayed
let data = ((10, "Monday"), (5, "Tuesday"), (15, "Wednesday"), (9, "Thursday"), (11, "Friday"))

// Create the necessary axes
let y_axis = axis(values: ("", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"), location: "left", show_markings: true)
let x_axis = axis(min: 0, max: 20, step: 2, location: "bottom", helper_lines: true)

// Combine the axes and the data and feed it to the plot render function.
let pl = plot(axes: (x_axis, y_axis), data: data)
bar_chart(pl, (100%, 33%), fill: (purple, blue, red, green, yellow), bar_width: 70%, rotated: true)
}

#graphtest()