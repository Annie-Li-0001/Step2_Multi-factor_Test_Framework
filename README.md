## **Multi-Factor System Optimization and Empirical Validation Under Dynamic Orthogonalization Framework**


### **Introduction**
In complex and dynamic market environments, constructing multi-factor models with strong predictive power and robustness remains the core objective of quantitative investment research. To this end, we have independently developed a dynamically-screened equity multi-factor analysis system that innovatively integrates factor orthogonalization, multi-collinearity control, and statistical validation frameworks, while implementing Python multi-process accelerated parallel computing architecture for efficient large-scale factor testing.

The system employs hierarchical orthogonalization rules (triple orthogonalization against industry, market capitalization, and Barra risk factors) to eliminate variable collinearity, coupled with a three-stage screening mechanism based on Rank IC mean, t-value testing, and directional consistency probability, significantly enhancing factor library purity and predictive stability. Specifically, we designed differentiated statistical threshold regimes: Barra factors prioritize stability verification (Rank IC mean >0.01, t>1.96), technical factors emphasize high predictive acuity (Rank IC >0.03, t>2.58), while fundamental factors adopt adaptive criteria. This tiered strategy effectively controls overfitting risks while fully exploiting heterogeneous alpha sources.

Empirical results demonstrate that our Python multi-process accelerated architecture reduces the full-cycle testing time for ten-thousand-factor sets to 17% of traditional methods. The generated dynamic evaluation report (containing IC/IR analysis, long-short portfolio backtesting, factor clustering heatmaps, etc.) provides an interpretable decision pathway for portfolio optimization. 

### **Installation**

This project already includes the required data, so there is no need to read external data. The data is solely for testing the framework. The project uses Python 3.11.

 Install conda, then create a virtual environment. 

 ```shell
 conda create --name=factor python=3.11
 ```
 
 After setting up the virtual environment, install the project dependencies.

 ```shell
pip install -r requirements.txt
 ```

 Start the framwork:
 ```shell
python main.py
 ```