const repo = 'jeffersonlab/jaws-libp';
const url = 'https://api.github.com/repos/' + repo + '/contents/?ref=gh-pages';

const list = document.getElementById('dirlist');

function sortSemVer(arr, reverse = false) {
    let semVerArr = arr.map(i => i.replace(/(\d+)/g, m => +m + 100000)).sort();
    if (reverse)
        semVerArr = semVerArr.reverse();

    return semVerArr.map(i => i.replace(/(\d+)/g, m => +m - 100000));
}

function addToList(name) {
  //console.log('addToList', name);

  const li = document.createElement("li");
  const a = document.createElement("a");
  a.href = name + '/';
  a.innerText = name;
  li.appendChild(a);
  list.appendChild(li);
  
}

async function fetchData() {
    //console.log('fetchData', url);


    const response = await fetch(url);

    const data = await response.json();

    //console.log(data);

    let dirs = data.filter(function(obj) {
       return obj.type === 'dir';
    });

    let names = dirs.map(i => i.name);

    sorted = sortSemVer(names, true);


    sorted.forEach(addToList);    
    
}

fetchData();
