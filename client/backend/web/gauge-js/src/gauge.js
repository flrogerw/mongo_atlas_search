export default class Gauge {
  /**
   * Initialize the gauge.
   * @param {string} emptyDivId the id of an empty div, constructor will fail if div is not completely empty with no newlines, tabs, etc. Example: <div></div>.
   * @param {object} options options for the gauge object to abide to.
   * @returns {Gauge}
   */
  constructor(emptyDivId, options = undefined) {
    this.options = {
      min: 0,
      max: 100,
      initialValue: 0,
      step: 0.1,
      textColor: "#000000AA",
      backgroundColor: "#FFFFFF",
      progressLeftColor: "#909090",
      initialColor: "#add8e6",
      finalColor: "#4169e1",
      displaySlider: true,
      transitionType: "linear",
      transitionTimeSeconds: 0.1
    }

    // Update default values with user-given properties.
    if (options !== undefined) {
      for (let property in options) {
        if (Object.prototype.hasOwnProperty.call(this.options, property)) {
          this.options[property] = options[property];
        }
      }
    }

    const initialColor = this.options.initialColor.replace("#", "");
    const finalColor = this.options.finalColor.replace("#", "");

    this.initialRGBValues = [
      parseInt((initialColor[0] + initialColor[1]), 16),
      parseInt((initialColor[2] + initialColor[3]), 16),
      parseInt((initialColor[4] + initialColor[5]), 16)
    ]

    this.finalRGBValues = [
      parseInt((finalColor[0] + finalColor[1]), 16),
      parseInt((finalColor[2] + finalColor[3]), 16),
      parseInt((finalColor[4] + finalColor[5]), 16)
    ]

    // Set current value
    if (this.options.initialValue > this.options.max) {
      this.currentValue = this.options.max;
    } else if (this.options.initialValue < this.options.min) {
      this.currentValue = this.options.min;
    } else {
      this.currentValue = this.options.initialValue;
    }
    
    const id = crypto.randomUUID();
    const emptyDiv = document.getElementById(emptyDivId);

    // Dumbass check #1
    if (emptyDiv.innerHTML !== "") {
      throw Error("the passed in div was not empty, please pass in an empty array for the gauge to write into.");
    }

    // For ease of getting live elements.
    const progressBarIdentifier = `progress-bar-${id}`;
    const sliderDisplayIdentifier = `slider-value-display-${id}`;
    const sliderIdentifier = `slider-${id}`;

    // Fill in empty div.
    emptyDiv.innerHTML = `
      <div id="gauge-cluster-${id}" style="width: 200px; overflow: hidden; position: relative;">
        <div id="gauge-container-${id}" style="height: 100px; width: 200px; overflow: hidden;">
          <div id="gauge-${id}" style="position: relative; width: 200px; height: 200px;">
            <div id="progress-container-before-${id}" style="position: absolute; content: ''; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 70%; height: 70%; background-color: ${this.options.backgroundColor}; border-radius: 50%; z-index: 200;"></div>
            <div id="progress-container-${id}" style="position: relative; width: 100%; height: 100%; border-radius: 50%; overflow: hidden">
              <div id="progress-left-${id}" style="position: absolute; width: 50%; height: 100%; background-color: ${this.options.progressLeftColor}; transform: rotate(90deg); transform-origin: center right; z-index: -1;"></div>
              <div id="${progressBarIdentifier}" style="position: absolute; width: 50%; height: 100%; transform: rotate(-90deg); background-color: ${this.options.progressColor}; transform-origin: center right; transition: transform ${this.options.transitionTimeSeconds}s ${this.options.transitionType}, background-color ${this.options.transitionTimeSeconds}s ${this.options.transitionType};"></div>
            </div>
            <div id="progress-container-after-${id}" style="position: absolute; content: ''; top: 50%; width: 100%; height: 50%; background-color: ${this.options.backgroundColor});"></div>
          </div>
        </div>
        <h3 id="${sliderDisplayIdentifier}" style="display: block; width: 100px; position: absolute; left: 50%; top: 60px; transform: translate(-50%); margin: 0; z-index: 250; text-align: center; font-family: sans-serif; font-size: 35px; color: ${this.options.textColor}; user-select: none;">0</h3>
        ${
          !this.options.displaySlider
          ?
          ""
          :
          `
            <div id="gauge-controls-${id}" style="margin-top: 5px;">
              <input id="${sliderIdentifier}" type="range" min="${this.options.min}" max="${this.options.max}" value="${this.options.value}" step="${this.options.step}" style="margin: 0; width: 100%;">
            </div>
          `
        }
      </div>
    `;

    // Store necessary live elements for live value changing, etc.
    this.slider = document.getElementById(sliderIdentifier);
    this.sliderDisplay = document.getElementById(sliderDisplayIdentifier);
    this.progressBar = document.getElementById(progressBarIdentifier);

    // Initialize variables so not messed up by cache
    this.#updateProgressBar(this.currentValue, this.options.min, this.options.max);
    this.#updateValueDisplay(this.currentValue, this.options.min, this.options.max);

    if (this.options.displaySlider) {
      this.#updateSlider(this.currentValue, this.options.min, this.options.max);

      // Set input event listener.
      this.slider.oninput = (event) => {
        const sliderValue = event.target.value;

        this.#updateProgressBar(sliderValue, this.options.min, this.options.max);
        this.#updateValueDisplay(sliderValue, this.options.min, this.options.max);
        this.#updateSlider(sliderValue, this.options.min, this.options.max);

        this.currentValue = sliderValue;
      }
    }
  }

  /**
   * Update the progress bar to reflect the gauge's value.
   * @param {number} value current value of the gauge.
   * @param {number} min min number of the gauge.
   * @param {number} max max number of the gauge.
   * @returns {void}
   */
  #updateProgressBar(value, min, max) {
    let tempMin = min;
    let tempMax = max;
    let rotation;

    if (min < 0) {
      tempMin = (min * -1);
    }

    if (max < 0) {
      tempMax = (max * -1);
    }

    const percentage = value / (tempMin + tempMax)

    // Ugly block of code :(
    const currentColorValues = [
      Math.floor(this.initialRGBValues[0] + ((this.finalRGBValues[0] - this.initialRGBValues[0]) * percentage)).toString(16).padStart(2, "0"),
      Math.floor(this.initialRGBValues[1] + ((this.finalRGBValues[1] - this.initialRGBValues[1]) * percentage)).toString(16).padStart(2, "0"),
      Math.floor(this.initialRGBValues[2] + ((this.finalRGBValues[2] - this.initialRGBValues[2]) * percentage)).toString(16).padStart(2, "0")
    ]

    const currentColor = "#" + currentColorValues[0] + currentColorValues[1] + currentColorValues[2];

    // Out-of-bounds guard
    if (value > max) {
      rotation = 90;
    } else if (value < min) {
      rotation = -90;
    } else {
      rotation = parseInt(((value/max) * 180)) - 90;
    }


    this.progressBar.style.backgroundColor = currentColor;
    this.progressBar.style.transform = `rotate(${rotation}deg)`;
  }

  /**
   * Update the value display to reflect the gauge's value.
   * @param {number} value current value of the gauge.
   * @param {number} min min number of the gauge.
   * @param {number} max max number of the gauge.
   * @returns {void}
   */
  #updateValueDisplay(value, min, max) {
    let displayValue;

    // Out-of-bounds guard
    if (value > max) {
      displayValue = max;
    } else if (value < min) {
      displayValue = min;
    } else {
      displayValue = value;
    }

    this.sliderDisplay.innerHTML = displayValue;
  }

  /**
   * Update the slider to reflect the gauge's value.
   * @param {number} value current value of the gauge.
   * @param {number} min min number of the gauge.
   * @param {number} max max number of the gauge.
   * @returns {void}
   */
  #updateSlider(value, min, max) {
    let sliderValue;

    // Out-of-bounds guard
    if (value > max) {
      sliderValue = max;
    } else if (value < min) {
      sliderValue = min;
    } else {
      sliderValue = value;
    }

    this.slider.value = sliderValue;
  }

  /**
   * Set the value of the gauge.
   * @param {number} value a number that you want the gauge to reflect.
   * @returns {void}
   */
  setValue(value) {
    this.currentValue = value;
    
    this.#updateProgressBar(value, this.options.min, this.options.max);
    this.#updateValueDisplay(value, this.options.min, this.options.max);
    
    if (this.options.displaySlider) {
      this.#updateSlider(value, this.options.min, this.options.max);
    }
  }

  /**
   * Returns the value of the gauge.
   * @returns {number}
   */
  getValue() {
    return this.currentValue;
  }
}