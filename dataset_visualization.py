import argparse
import json
import os
import webbrowser
from utils.dataset import load_source_dataset
from utils.display import table_visualization


VISUALIZATION_FORMAT = """
<html>
    <head>
        <title>Dataset visualization</title>
        <style>
            .content { display: none; }
            .active { display: block; }
            .navigation { margin-top: 20px; }
        </style>
        <script>
            let currentIndex = 0;
            function showContent(index) {
                const contents = document.getElementsByClassName('content');
                for (let i = 0; i < contents.length; i++) {
                    contents[i].classList.remove('active');
                }
                contents[index].classList.add('active');
            }
            function nextContent() {
                const contents = document.getElementsByClassName('content');
                currentIndex = (currentIndex + 1) % contents.length;
                showContent(currentIndex);
            }
            function prevContent() {
                const contents = document.getElementsByClassName('content');
                currentIndex = (currentIndex - 1 + contents.length) % contents.length;
                showContent(currentIndex);
            }
            window.onload = function() {
                showContent(currentIndex);
            }
        </script>
    </head>
    <body>
    <div class="navigation">
        <button onclick="prevContent()">Previous</button>
        <button onclick="nextContent()">Next</button>
    </div>
    """


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceMultiTabQA', 'SourceOpenWikiTable'], help='dataset name')
    args, _ = parser.parse_known_args()

    CLASS = {
        'SourceMultiTabQA': 'low_header_sim',
        'SourceOpenWikiTable': 'high_header_sim'
    }

    from steps.regularize import regularize_source_dataset
    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=args.d))
    classification = CLASS[args.d]

    # ===

    table_lake = {table['id']: table for table in source_dataset.tables}
    with open(f'results/{classification}_dataset.json', 'r') as file:
        dataset = json.load(file)

    html_file_path = f'results/{classification}_dataset_visualization.html'

    # ===

    with open(html_file_path, 'w') as html_file:
        html_file.write(VISUALIZATION_FORMAT)

        for idx, data in enumerate(dataset):
            html_file.write(f'<div class="content" id="content-{idx + 1}">')

            for table_index, table_id in enumerate(data['gold_table_id_set']):
                table_html = table_visualization(
                    table_num=table_index + 1,
                    metadata=table_lake[table_id]['metadata'],
                    header=table_lake[table_id]['header'],
                    cell=table_lake[table_id]['cell']
                )
                html_file.write(f"<div>{table_html}</div><br><br>")
            
            html_file.write(f"<h3>Question: {data['question']}</h3>")
            html_file.write(f"<h4>Answer: {data['answer']}</h4>")
            html_file.write("<hr></div>")  # 각 데이터 구분선
        
        html_file.write("</body></html>")

    webbrowser.open(f"file://{os.path.abspath(html_file_path)}")
