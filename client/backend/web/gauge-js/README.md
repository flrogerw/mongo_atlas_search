# Gauge.js

A vanilla javascript gauge class for use in a UI.

## Getting Started

There are two ways to use this module: in another JavaScript file, or within 
the script tag in HTML.

### If using in JavaScript

Import the module into your Javascript file. NOTE: This is using ES6 syntax as 
opposed to CommonJS, feel free to change the export to CommonJS if that fits 
the scenario better.

```
import Gauge from 'your/path/to/gauge.js';

const gauge = new Gauge("empty-div-id"); // Uses default settings
```

### If using in HTML

Add the module into your html document as a script then access the class within 
another script element.

```
<div id="empty-div-id"></div>
<script src="your/path/to/gauge.html.js"></script>
<script>
  const gauge = new Gauge("empty-div-id"); // Uses default settings
</script>
```

## Options for gauge

The Gauge class has two parameters: the id string of an empty div that is present in
the linked HTML document, and an options object.

The possible options and their default values for the Gauge are as follows:
```
{
  min: 0,                           // {number}, minimum value of the gauge.
  max: 100,                         // {number}, maximum value of the gauge.
  initialValue: 0,                  // {number}, initial value of the gauge to display on render.
  step: 0.1,                        // {float}, step value for slider.
  textColor: "#000000aa",           // {string}, display value color.
  backgroundColor: "#ffffff",       // {string}, gauge middle piece color, this should be 
                                    // the color of whatever is behind the gauge. (Not an elegant solution at all)
  progressLeftColor: "#909090",     // {string}, color of the empty portion of the progress bar.
  initialColor: "#add8e6",          // {string}, initial color of the progress bar -- when the progress bar nears its minimum.
  finalColor: "#4169e1",            // {string}, final color of the progress bar -- when the progress bar nears its maximum.
  displaySlider: true,              // {boolean}, whether to display the slider or not.
  transitionType: "linear",         // {string}, formula for progress bar movement.
  transitionTimeSeconds: 0.1,       // {float}, time for transition to complete.
}
```

### Examples

```
<div id="empty-div-id"></div>
<script src="your/path/to/gauge.html.js"></script>
<script>
  const gauge = new Gauge("empty-div-id", {
    max: 300,
    step: 5,
  });
</script>
```

OR

```
<div id="empty-div-id"></div>
<script src="your/path/to/gauge.html.js"></script>
<script>
  const gauge = new Gauge("empty-div-id", {
    displaySlider: false,
    transitionType: "cubic-bezier(.11,.68,.29,.9)",
    transitionTimeSeconds: 0.7,
  });
</script>
```

## Methods

The gauge class currently has 2 methods.

### setValue

This method allows the user to feed data to the gauge without needing to provide input 
to the slider.

| Parameter | Type   | Description                        |
|-----------|--------|------------------------------------|
| value     | number | The value for the gauge to reflect |

Returns {void}

#### Example
```
<div id="empty-div-id"></div>
<script src="your/path/to/gauge.html.js"></script>
<script>
  const gauge = new Gauge("empty-div-id", {
    displaySlider: false,
    transitionType: "cubic-bezier(.11,.68,.29,.9)",
    transitionTimeSeconds: 0.7,
  });

  gauge.setValue(10); // The gauge will reflect this value assuming it is within 
                      // the minimum and maximum values.
</script>
```

### getValue

This method allows the user to retrieve the data from the gauge to pass into other.

| Parameter | Type   | Description                        |
|-----------|--------|------------------------------------|
| null      | null   | No parameters to pass.             |

Returns {number};

#### Example

```
<div id="empty-div-id"></div>
<script src="your/path/to/gauge.html.js"></script>
<script>
  const gauge = new Gauge("empty-div-id", {
    displaySlider: false,
    initialValue: 13
  });

  const gaugeValue = gauge.getValue(); // Method will return the current value of the gauge.
  console.log(gaugeValue);             // Output: 13
</script>
```

## Credits

This is a heavily modified gauge from [Jeet Saru's](https://github.com/JeetSaru) GitHub repo [Build-Gauge-With-HTML-CSS-JS](https://github.com/JeetSaru/Build-Gauge-With-HTML-CSS-JS).

## License

Copyright 2024 Andrew Williams (andrewdotjs)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
