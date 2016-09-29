<div class="cmdhead"></div>

<div class="description"></div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="subsection">

Conceptually, this command's output is a symmetric, sample-by-sample matrix. The
implementation is based on the IBD algorithm described in
[the PLINK paper](http://www.ncbi.nlm.nih.gov/pmc/articles/PMC1950838/).

This command assumes the dataset is bi-allelic. This command does not perform LD
pruning but linkage disequilibrium may negatively influence the results.

#### TSV format

The `--output` flag triggers TSV output. The TSV does not contain headers. The
columns are, in order: the first sample id, the second sample id, `Z0`, `Z1`,
`Z2`, `PI_HAT`. For example:

```
sample1	sample2	1.0000	0.0000	0.0000	0.0000
sample1	sample3	1.0000	0.0000	0.0000	0.0000
sample1	sample4	0.6807	0.0000	0.3193	0.3193
sample1	sample5	0.1966	0.0000	0.8034	0.8034
    ⋮
```

#### Examples

```
... ibd --minor-allele-frequency 'va.mafs[v]' -o ibd.tsv
```

This invocation writes the full IBD matrix to `ibd.tsv`. It also uses the
provided allele frequencies.