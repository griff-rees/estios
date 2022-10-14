# The Model

| Variable   | Definition              |
| --------   | ------------------------|
| $P_i$      | population of city $i$  |
| $Q_m^i$    | employment in sector $m$ in city $i$ |
| $X_m^i$    | total production of sector $m$ in city $i$ where <br /> $m = 1, 2, 3, …, S$ and $i = 1, 2, 3, …, N$ |
| $F_m^i$    | final demands for $m$ in $i$ |
| $e_m^i$    | total exports of $m$ from $i$ to other cities (excluding international exports) |
| $m_m^i$    | total imports of $m$ into $i$ from other cities (excluding international imports) |
| $E_m^i$    | international exports of $m$ from city $i$ |
| $M_m^i$    | international imports of $m$ to city $i$ |
| $x_{mn}^i$ | the total demand for $m$ to produce a unit of $n$ in $i$ |
| $y_{mi}^j$ | the trade flow (internal export) of $m$ from city $i$ to city j; <br /> or $y_{mj}^i$ is the internal import flow to $i$. (note the inversion of $j$ and $i$) |

We can then define

```math
a_{mn}^i
```

as the amount of $m$ needed to produce a unit of $n$ in $i$ , so that

### Equation 1

```math
x_{mn}^i = a_{mn}^i X_n^i
```

which is the last component of the overall macro-economic model demonstrated in equation 2:

### Equation 2

```math
X_m^i + m_m^ i + M_m^i = F_m^i + e_m^i + E_m^i +\sum_n{a_{mn}^i X_n^i}
```

where the total production of $m$ in $i$ + imports from other cities + international imports = final demand + exports to other cities + international exports + intermediate demand

### Equation 3

As definied above, $X_m^i$ is the total production of sector $m$ in city $i$, and we can estimate that from national totals with

```math
X_m^i = X_*^m * Q_m^i / Q_{*}^m
```

where $X_*^m$ and $Q_*^m$ are the macro equivalents to estimate with respect to (so far, $X_*^m$ and $Q_*^m$ are national levels of production and employment respectively).
