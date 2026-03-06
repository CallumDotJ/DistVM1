
/* Types Loaders */

//load types into the select menu
async function loadTypes()
{
    const select = document.getElementById("jokeTypeSelect");

    try{
        const response = await fetch("/types") //fetch
        const types = await response.json();

        select.innerHTML = '<option value="">Select a type</option>'; //clear all besides placeholder

        types.types.forEach(type => { // loop and insert options into the select menu
            const option = document.createElement("option");
            option.value = type;
            option.textContent = type;
            select.appendChild(option);
        });
    }
    catch(error)
    {
        console.error("Error loading joke types:", error);
    }
}

//load types on page load and register dynamical refresher // NOW USING defer in HTML
document.addEventListener("DOMContentLoaded", () => {

    const select = document.getElementById("jokeTypeSelect");

    loadTypes(); 
    select.addEventListener("focus", loadTypes); // refresh on user interaction
});


/* Joke Fetcher */

const jokeSetup = document.getElementById("jokeSetup");
const jokePunchline = document.getElementById("jokePunchline");

async function fetchJoke()
{
    const count = document.getElementById("jokeCount").value;
    const type = document.getElementById("jokeTypeSelect").value;

    try
    {
        const response = await fetch(`/joke/${type}?count=${count}`);

        if(!response.ok) // catch api level errors
        {
            console.error("Error fetching joke:", response.statusText);
            return;
        }
        

        const data = await response.json();

        console.log(data); //debug log

        const firstJoke = data.jokes?.[0]; // just first incase of multiple

        if(!firstJoke) // catch no joke returned
        {
            console.error("No jokes returned from API");
            jokeSetup.textContent = "No joke available.";
            jokePunchline.textContent = "";
            return;
        }

        //display joke

        jokeSetup.textContent = firstJoke.setup;
        jokePunchline.textContent = "";

        //reveal punchline after delay
        setTimeout(() => {
            jokePunchline.textContent = firstJoke.punchline; 
        }, 3000); // 3 second delay

    }
    catch(error)
    {
        console.error("Error fetching joke:", error);
    }
}


document.getElementById("newJokeBtn").addEventListener("click", fetchJoke); // check for if need to be in within DOMContentLoaded