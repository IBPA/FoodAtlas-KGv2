import pandas as pd


if __name__ == '__main__':
    data = pd.read_excel(
        "food_atlas/additional_analysis/without_concentration_benchmark.xlsx"
    )
    data = data[['Unnamed: 7', 'Unnamed: 9', 'Unnamed: 11']]
    data.columns = ['BioBERT', 'GPT-3.5', 'GPT-4']

    def parse_metrics(x):
        res = {}
        metrics = x.split('\n')
        metrics = [m.split(':') for m in metrics]

        for k, v in metrics:
            if k not in ['TP', 'FP', 'FN']:
                raise ValueError(f"Unknown metric: {k}")

            if v.strip():
                res[k] = int(v.strip())
            else:
                res[k] = 0

        if len(res) != 3:
            raise ValueError(f"Invalid number of metrics: {len(res)}")

        return res

    data = data.map(parse_metrics)

    def summarize(col):
        tp = sum([x['TP'] for x in col])
        fp = sum([x['FP'] for x in col])
        fn = sum([x['FN'] for x in col])

        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        f1 = 2 * precision * recall / (precision + recall)

        return {
            'TP': tp,
            'FP': fp,
            'FN': fn,
            'Precision': precision,
            'Recall': recall,
            'F1': f1
        }

    data = data.apply(summarize).apply(pd.Series)
    print(data)