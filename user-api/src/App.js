import { useState, useEffect } from "react";
import logo from "./logo.svg";
import "./App.css";
import { ReactSearchAutocomplete } from "react-search-autocomplete";

function App() {
  const [indexes, setIndexes] = useState([]);
  const [selected, setSelected] = useState("");
  const fields = [];

  useEffect(() => {
    fetch("http://localhost:5000/getindexes")
      .then((response) => response.json())
      .then((data) => {
        setIndexes(data["indexes"]);
      });
  }, []);

  useEffect(() => {
    if (!selected) {
      return;
    }

    const json = {
      index: selected,
    };

    fetch("http://localhost:5000/getfields", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(json),
    })
      .then((response) => response.json())
      .then((data) => {
        console.log(data);
        data["fields"].map((field) => {
          <ReactSearchAutocomplete key={field} />;
        });
      });
  }, [selected]);

  const handleSelect = (event) => {
    setSelected(event.target.value);
    console.log(event.target.value);
  };

  const onSearch = (string, results) => {
    // onSearch will have as the first callback parameter
    // the string searched and for the second the results.
    console.log(string, results);
  };

  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <div>
          {indexes.map((index) => (
            <label key={index}>
              <input
                type="radio"
                value={index}
                checked={selected === index}
                onChange={handleSelect}
              />
              {index}
              <br />
            </label>
          ))}
        </div>
      </header>
    </div>
  );
}

export default App;
