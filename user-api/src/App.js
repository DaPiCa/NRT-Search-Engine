import logo from "./logo.svg";
import "./App.css";
import React, { Component } from "react";
import { Dna } from "react-loader-spinner";

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      indexes: [],
      fields: [],
      selectedIndex: null,
    };
  }

  selectedIndexChanged = async (event) => {
    this.setState({ selectedIndex: event.target.value }); // Actualizar el estado con el índice seleccionado
    console.log(event.target.value);
    const json = { index: event.target.value };
    /* Make a fetch awaiting for response before continue */
    fetch("http://localhost:5000/getfields", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(json),
    })
      .then((res) => res.json())
      .then(async (fields) => {
        await this.setState({ fields: fields["fields"] });
      });
  };

  componentDidMount() {
    fetch("http://localhost:5000/getindexes")
      .then((res) => res.json())
      .then((indexes) => this.setState({ indexes: indexes["indexes"] }));
  }

  parseFields = (field) => {
    // Utilizar expresiones regulares para dividir el campo en palabras
    const words = field.split(/(?=[A-Z0-9])|_/);

    // Reemplazar guiones bajos con espacios en cada palabra y convertir la primera letra a mayúscula
    const formattedWords = words.map((word) => {
      // Si es un número, retornarlo tal cual
      if (/^\d+$/.test(word)) {
        return word;
      }
      // Si es una palabra, reemplazar guiones bajos con espacios y convertir la primera letra a mayúscula
      else {
        return word
          .replace(/_/g, " ")
          .replace(/^(.)(.*)$/, (match, p1, p2) => `${p1.toUpperCase()}${p2}`);
      }
    });

    // Unir las palabras con espacios y retornar el resultado
    return formattedWords.join(" ");
  };

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          {this.state.fields.length > 0 ? (
            this.state.fields.map((field) => (
              <div key={field}>
                <label htmlFor={field}>{this.parseFields(field)}</label>
                <input type="text" id={field} name={field}></input>
              </div>
            ))
          ) : (
            <div className="App-spinner">
              <Dna
                visible={true}
                height="80"
                width="80"
                ariaLabel="dna-loading"
                wrapperStyle={{}}
                wrapperClass="dna-wrapper"
              />
              <br />
              <h7>Waiting for user selection...</h7>
            </div>
          )}
          {/* Create one label per index with a radio button if there is elements in the list. Only one can be selected at a time*/}
          <div className="App-sidebar">
            {this.state.indexes.length > 0 ? (
              this.state.indexes.map((index) => (
                <div key={index}>
                  <input
                    type="radio"
                    id={index}
                    name="answer"
                    value={index}
                    onChange={this.selectedIndexChanged}
                  ></input>
                  <label htmlFor={index}>{index}</label>
                </div>
              ))
            ) : (
              /* Render Dna element centered on the div */
              <div className="App-spinner">
                <Dna
                  visible={true}
                  height="80"
                  width="80"
                  ariaLabel="dna-loading"
                  wrapperStyle={{}}
                  wrapperClass="dna-wrapper"
                />
                <br />
                <h7>Loading avaliable indexes...</h7>
              </div>
            )}
          </div>
        </header>
      </div>
    );
  }
}

export default App;
