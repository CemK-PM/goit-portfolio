# 📊 SaaS Onboarding Funnel & Retention Analysis

> **Executive Summary:** Analyzing user journey friction and long-term retention patterns to optimize product onboarding.

---

### 🚀 Key Insights
| Metric | Achievement |
| :--- | :--- |
| **Funnel Efficiency** | **58%+ completion rate** for core media-play action. |
| **Platform Variance** | **iOS** sessions averaged **46 mins longer** than Android. |
| **Engagement** | Analyzed active user dynamics for specific cohorts (~2.25k users) to monitor daily engagement trends using Amplitude. |

---

### 🛠 Tech Stack
* **Analysis:** SQL (BigQuery), Amplitude, GA4
* **Visualization:** Amplitude
* **Metrics:** Conversion Rate, Session Duration

---

### 📈 Visualizations & Data

#### 1. User Onboarding Funnel (Amplitude)
*Mapped onboarding for 34.6k Android and 29.3k iOS users with a 58%+ final conversion rate.*
![Funnel Visualization](./Users_Onboarding_and_Retention_Analysis%20(2).png)

#### 2. User Growth & Engagement Trends (Amplitude)
*Analyzed daily unique user dynamics, showing a 3.08% upward trend in active engagement.*
![User Trends](./Cohort%20(1).png)

---

### 🧾 Technical Implementation
**SQL Funnel Query (GA4 + BigQuery)**
Tracked step-by-step funnel events from add-to-cart to purchase and calculated conversion rates.
👉 [**View SQL Query Source Code**](./ga4_funnel_full_query_from_user%20(1).sql)
