
const size = document.getElementById("size");
const size_display = document.getElementById("size_display");
const results = document.getElementById("results");
const form = document.getElementById("upload-form");
const result_count = document.getElementById("result_count");
const xhr = new XMLHttpRequest();

const search_phrase = document.getElementById("search_phrase");
const list = document.getElementById('search_as_you_type_results');

function select_dropdown(e) {
    search_phrase.value = e.target.getAttribute("search_phrase")
    do_search()
}

search_phrase.addEventListener("focus", (e) => {list.style.display = "block"});
search_phrase.addEventListener("keyup", (e) => {
    params = {search_phrase: search_phrase.value, index: 'search_queries', size: 10}
    queryString = new URLSearchParams(params).toString();
    xhr.open("get", `/search_as_you_type?${queryString}`);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function() {
        if (xhr.readyState === XMLHttpRequest.DONE) {
            if (xhr.status === 200) {
                data = JSON.parse(xhr.responseText);
                list.innerHTML = ''
                for (let i in data.results.slice(0, 15)) {
                    let option = document.createElement('div');
                    option.setAttribute("search_phrase", data.results[i].fields.search_query[0])
                    option.className = "drop_down_option";
                    option.addEventListener("click", select_dropdown, false);
                    option.innerHTML = data.results[i].highlight.search_query[0];
                    list.appendChild(option);
                }

      } else {
        console.log(JSON.parse(xhr.responseText));
      }
    }
  };
    xhr.send();
}, false);

function do_search() {
  results.innerHTML = '';
  list.innerHTML = '';
  queryString = new URLSearchParams(new FormData(form)).toString();
  actionPath = form.getAttribute("action");

  xhr.open("get", `${actionPath}?${queryString}`);
  xhr.setRequestHeader('Content-Type', 'application/json');
  xhr.onreadystatechange = function() {
    if (xhr.readyState === XMLHttpRequest.DONE) {
      if (xhr.status === 200) {
        endTime = performance.now();
        data = JSON.parse(xhr.responseText);
        results.scrollTop = 0;
        results.style.display = "block";
        for (let i in data.results.slice(0, 15)) {
          const result = data.results[i];
          const div = document.createElement("div");
          div.setAttribute("class", "ResultContainer");
          const title = document.createElement("div");
          title.setAttribute("class", "Title");
          title.innerHTML = (result.title) ? result.title : result.fields.title_cleaned[0];
          div.appendChild(title);
          const description = document.createElement("div");
          description.setAttribute("class", "Description");
          description.innerHTML = result.fields.description_cleaned[0];
          div.appendChild(description);
          results.append(div)
        }
      } else {
        console.log(JSON.parse(xhr.responseText));
      }
    }
  };
  xhr.send();
}


form.addEventListener("submit", (e) => {
    e.preventDefault();
    do_search();
 });

// Get the modal
var modal = document.getElementById("myModal");

// Get the button that opens the modal
var btn = document.getElementById("myBtn");

// Get the <span> element that closes the modal
var span = document.getElementsByClassName("close")[0];

// When the user clicks on <span> (x), close the modal
span.onclick = function() {
  modal.style.display = "none";
}

// When the user clicks anywhere outside of the modal, close it
window.onclick = function(event) {
if (event.target != search_phrase) {
    list.style.display = "none";
  }
  if (event.target == modal) {
    modal.style.display = "none";
  }
}


window.onload = function(event) {
    modal.style.display = "block";
}