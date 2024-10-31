let isLoading = false;

let classification = "";
let dataset = [];

let currentIndex = 0;

function loadData(classify) {
    if (isLoading) return;

    classification = classify;

    isLoading = true;

    const buttons = document.querySelectorAll("#load-buttons button");
    buttons.forEach(button => button.disabled = true);

    fetch(`https://kwondu.github.io/mtr-lftqa-dataset/results/${classification}_dataset.json`).then(response => response.json())
        .then((dataSet) => {
            dataset = dataSet;
            currentIndex = 0;
            renderData(currentIndex);
        })
        .catch(error => console.error("[Error] loading data:", error))
        .finally(() => {
            isLoading = false;
            buttons.forEach(button => button.disabled = false);
        });
}

async function renderData(index) {
    const container = document.getElementById("dataset-container");
    container.innerHTML = "<p>Loading...</p>";

    if (index < 0 || index >= dataset.length) {
        container.innerHTML = "<p>No data available.</p>";
        return;
    }

    const data = dataset[index];
    const question = data['question'];
    const translatedQuestion = data['translated_question'];
    const answer = data['answer'];
    const translatedAnswer = data['translated_answer'];
    const goldTableIDSet = data['gold_table_id_set'];

    const tableContainer = document.createElement("div");
    tableContainer.classList.add("table-container");

    const tableFetchPromises = goldTableIDSet.map((table_id, idx) =>
        fetch(`https://kwondu.github.io/mtr-lftqa-dataset/results/table_lake/${classification}_table_${table_id}.json`).then(response => response.json())
            .then((table) => {
                const tableDiv = document.createElement("div");
                tableDiv.classList.add("table-responsive", "mb-4");

                const title = document.createElement("h4");
                title.textContent = `Table ${idx + 1}: ${table['metadata']}`;
                tableDiv.appendChild(title);

                const tableEl = document.createElement("table");
                tableEl.classList.add("table", "table-bordered", "table-striped");

                const thead = document.createElement("thead");
                const headerRow = document.createElement("tr");
                table['header'].forEach(header => {
                    const th = document.createElement("th");
                    th.textContent = header;
                    headerRow.appendChild(th);
                });
                thead.appendChild(headerRow);
                tableEl.appendChild(thead);

                const tbody = document.createElement("tbody");
                table['cell'].forEach(row => {
                    const tr = document.createElement("tr");
                    row.forEach(cellData => {
                        const td = document.createElement("td");
                        td.textContent = cellData;
                        tr.appendChild(td);
                    });
                    tbody.appendChild(tr);
                });
                tableEl.appendChild(tbody);

                tableDiv.appendChild(tableEl);
                return tableDiv;
            })
            .catch(error => console.error("[Error] loading table:", error))
    );

    const tableDivs = await Promise.all(tableFetchPromises);
    tableDivs.forEach(tableDiv => tableContainer.appendChild(tableDiv));

    const qaSection = document.createElement("div");
    qaSection.classList.add("mb-3");
    qaSection.innerHTML = `
        <h3>Question</h3>
        <p>${question}</p>
        ${translatedQuestion ? `<h3>Translated Question</h4><p>${translatedQuestion}</p>`: ""}
        <h3>Answer</h3>
        <p>${answer}</p>
        ${translatedAnswer ? `<h3>Translated Answer</h4><p>${translatedAnswer}</p>`: ""}
    `;

    container.innerHTML = '';
    container.appendChild(tableContainer);
    container.appendChild(qaSection);

    updateIndexDisplay();
}

function showNext() {
    currentIndex = (currentIndex + 1) % dataset.length;
    renderData(currentIndex);
}

function showPrev() {
    currentIndex = (currentIndex - 1 + dataset.length) % dataset.length;
    renderData(currentIndex);
}

function updateIndexDisplay() {
    const indexDisplay = document.getElementById("data-index");
    indexDisplay.textContent = `${currentIndex + 1} / ${dataset.length}`;
}
