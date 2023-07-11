import { useState, useContext } from 'react';
import './App.css';

export default function App() {

  const [indexListed, setIndexListed] = useState(false);
  const [fieldsListed, setFieldsListed] = useState(false);
  const [bool_search, setSearch] = useState(false);
  const [base_index, setIndex] = useState([]);
  const [indexSelected, setIndexSelected] = useState("");
  const [fields, setFields] = useState([]);
  const [searched, setSearched] = useState([]);

  if (!indexListed) {
    fetch('http://localhost:5001/getindexes')
      .then(response => response.json())
      .then(data => {
        setIndex(data["indexes"]);
        setIndexListed(true);
      });
  }

  if (indexSelected !== "" && !fieldsListed) {
    let json = {
      "index": indexSelected
    }
    fetch('http://localhost:5001/getfields', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(json)
    })
      .then(response => response.json())
      .then(data => {
        setFields(data["fields"]);
        setFieldsListed(true);
      });
  }


  let search = (event, index, field) => {
    console.log(event.target.value);
    let json = {
      "index": indexSelected,
      "field": field,
      "query": event.target.value
    }
    fetch('http://localhost:5001/search', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(json)
    })
      .then(response => response.json())
      .then(data => {
        console.log(data);
        setSearched(data);
        setSearch(true);
      });
  }

  return (
      <div className="parent">
        <div className="div1">
          {base_index.map((item, index) => (
            <input type="button" className="button" key={index} value={item} onClick={() => {
              setIndexSelected(item);
              setFieldsListed(false);
              setSearch(false);
              setSearched([]);
            }} />
          ))}
        </div>
        <div className="div2">
          {fields && fields.map((item, index) => (
            <div key={index}>
              <label>{item}</label>
              <br></br>
              <input type="text" className="searcher" onChange={(event) => search(event, index, item)} />
            </div>
          ))}
        </div>
        <div className="div3">
          {bool_search && <h1>Resultados:</h1>}
          {searched && searched.map((item, index) => (
            <>
              <div key={index} className='element'>
                {Object.entries(item["_source"]).map(([key, value]) => (
                  <div key={key}>
                    <label>{key}: </label>
                    <span>{value}</span>
                  </div>
                ))}
              </div>
            </>
          ))}
        </div>
      </div>
  );
}