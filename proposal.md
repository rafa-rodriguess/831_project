# Paper Proposal (Final Version – With APA References)

## From Clicks to Constructs: A Mixed-Methods Framework for Modeling Latent Student Engagement Using OULAD Data

⸻

## 1. Background and Motivation

Student engagement is widely recognized as a central determinant of learning outcomes in higher education. However, its measurement remains inherently challenging due to its multidimensional nature, encompassing behavioral, cognitive, and affective components (Sinatra et al., 2015). In digital learning environments, particularly those mediated by Learning Management Systems (LMS), engagement is typically approximated through observable behavioral traces such as clicks, logins, and resource interactions.

While these data provide valuable insights, they represent only partial proxies of engagement. Prior work has emphasized that reliance on LMS log data alone risks overlooking key aspects of the student experience, including motivation, perception, and cognitive effort (Wong & Chong, 2018; Sinatra et al., 2015). At the same time, qualitative approaches—such as focus groups and interviews—capture these dimensions but often lack the analytical structure needed to connect them to observable behavior (Johnson et al., 2007).

Mixed methods research offers a promising path forward by integrating quantitative and qualitative perspectives (Nguyen et al., 2020). However, existing applications in educational research frequently fall short of achieving true integration, instead presenting parallel analyses without a unifying interpretive framework (Viberg et al., 2018).

To address these limitations, this study proposes a novel framework that conceptualizes student engagement as a latent construct, inferred through the integration of behavioral data from LMS logs and qualitative insights into student experience. Using the Open University Learning Analytics Dataset (OULAD), this research seeks to move beyond proxy-based measurement toward a more comprehensive and interpretable model of engagement.

⸻


⸻

## Part 1: The Scenario

This study is situated in a real-world higher education scenario in which universities rely on large-scale Learning Management System (LMS) data to understand student engagement. In contemporary digital learning environments, platforms such as Moodle generate continuous and high-volume behavioral traces, including clicks, resource access, timestamps, and assessment-related actions. Within educational settings, these data function as a form of big data because they are behaviorally granular, temporally structured, and collected across large student populations.

The dominant sociotechnical imaginary in this scenario is the belief that digital traces can make student engagement fully visible, measurable, and manageable. Under this narrative, LMS platforms are treated as infrastructures capable of rendering student engagement legible through activity logs, access patterns, and interaction intensity. This approach promises scalability, apparent objectivity, and institutional efficiency, suggesting that if all student behaviors in the LMS are captured, then engagement can be adequately measured and acted upon.

However, this assumption produces a significant form of **data silence**. Although LMS logs may capture all available online interactions, they do not directly capture key dimensions of engagement such as motivation, attention, cognitive effort, confusion, perceived value of learning tasks, emotional response, or offline study behavior. In this context, **“N = all” fails** because it refers only to all observable platform traces rather than to the full educational phenomenon under investigation. The issue is therefore not simply one of sample size, but of epistemic incompleteness: the data may be extensive, yet the phenomenon remains only partially observable.

Against this background, the central research question of this proposal is: **How can student engagement in online higher education be modeled as a latent construct by integrating LMS behavioral data with qualitative accounts of student experience?**

⸻

## Part 2: The Design

To address this problem, the study adopts a **convergent mixed-methods design** in which quantitative and qualitative strands are conducted in parallel and then integrated through the **Latent Engagement Joint Display (LE-JD)**. This design is appropriate because student engagement is only partially visible in each strand and must therefore be reconstructed through integration.

The quantitative component uses an advanced data science approach based on a **Dynamic Bayesian Network (DBN)**. This method is well suited to the study because engagement is not a static variable but a temporal process that unfolds through repeated interactions with the learning environment. Using OULAD, clickstream data from the `studentVle` table will be transformed into a person–week panel dataset based on fields such as `id_student`, `code_module`, `code_presentation`, `date`, and `sum_click`. From these variables, the analysis will derive indicators such as weekly activity, interaction intensity, recency, and streak. Additional contextual variables from `studentAssessment` and `studentInfo`, including `score`, `date_submitted`, `final_result`, `age_band`, `gender`, `highest_education`, `imd_band`, `num_of_prev_attempts`, and `studied_credits`, will be used to contextualize engagement trajectories. The DBN will model probabilistic dependencies and temporal transitions among engagement-related indicators, allowing the study to identify structural and temporal patterns in student behavior.

The qualitative component uses **semi-structured interviews**, analyzed through **thematic analysis**, to identify mechanisms underlying the behavioral patterns found in the quantitative strand. This method is appropriate because it allows students to explain how they experience learning activities, how they interpret the value of tasks, how they manage workload, and how they engage in both visible and invisible forms of study. Through thematic analysis, the qualitative strand will identify mechanisms such as motivation, perceived value, habit formation, strategic compliance, and cognitive overload.

Integration occurs through the **Latent Engagement Joint Display (LE-JD)**, which aligns behavioral indicators, inferred latent engagement states, qualitative mechanisms, and meta-inferences. In methodological terms, the LE-JD serves not only as a reporting device but also as an analytical framework for integration, making explicit how quantitative patterns and qualitative explanations converge, diverge, or expand one another. In this way, the design directly addresses the limitations of relying on LMS behavioral data alone and supports a more comprehensive model of student engagement.

## 2. Research Problem

Despite the growing adoption of learning analytics, three fundamental limitations persist.

First, engagement is often operationalized through observable proxies—such as clicks or submissions—without a formal definition of the underlying construct. This leads to conceptual ambiguity and limits interpretability.

Second, probabilistic models such as Bayesian networks are frequently used to identify relationships in student behavior (Scutari, 2010; Reichenberg, 2018), yet these relationships are inherently associational rather than causal. Without explicit acknowledgment of this limitation, there is a risk of overinterpretation.

Third, although mixed methods approaches are recognized as valuable for understanding complex educational phenomena (Johnson et al., 2007; Nguyen et al., 2020), many studies fail to integrate quantitative and qualitative findings in a structured and theoretically grounded manner.

This study addresses these gaps by proposing a unified framework that explicitly distinguishes between observable behavior, latent engagement, and underlying mechanisms.

⸻

## 3. Research Objectives

The primary objective of this study is to develop and validate a framework for modeling student engagement as a latent construct using OULAD data, while integrating qualitative insights to explain observed behavioral patterns.

Specifically, this study aims to:
- Model engagement as a latent construct inferred from behavioral indicators derived from LMS data
- Identify temporal and structural patterns of engagement using probabilistic modeling
- Elicit mechanisms underlying engagement through qualitative analysis
- Integrate quantitative and qualitative findings into a unified analytical artifact
- Provide actionable insights for pedagogical design and evaluation

⸻

## 4. Research Questions

This study is guided by the following research questions:
- RQ1: How can student engagement be modeled as a latent construct using behavioral data from OULAD?
- RQ2: What temporal and structural patterns characterize engagement trajectories?
- RQ3: What mechanisms explain observed engagement behaviors?
- RQ4: How can quantitative and qualitative evidence be integrated into a unified representation of engagement?

⸻

## 5. Data Source: OULAD

This study utilizes the Open University Learning Analytics Dataset (OULAD), which provides detailed information on student demographics, assessment outcomes, and interaction with virtual learning environments.

Behavioral data will be derived from the studentVle table, including:
- id_student, code_module, code_presentation
- date (timestamp of interaction)
- sum_click (interaction intensity)

These variables enable the construction of temporal engagement indicators such as activity patterns, recency, and persistence.

Academic engagement will be captured using the studentAssessment table:
- score
- date_submitted

Demographic and contextual variables will be drawn from studentInfo, including:
- age_band, gender, highest_education
- imd_band (socioeconomic indicator)
- num_of_prev_attempts, studied_credits

Finally, outcomes such as final_result (e.g., pass, fail, withdrawn) will be used to contextualize engagement trajectories.

⸻

## 6. Methodology

### 6.1 Research Design

As outlined in **Part 2: The Design**, this study adopts a **convergent mixed-methods design** in which quantitative and qualitative strands are developed in parallel and integrated at the interpretation stage through the **Latent Engagement Joint Display (LE-JD)**. The methodological logic of this design is to treat student engagement as a partially observable phenomenon that cannot be adequately captured through either behavioral logs or qualitative accounts alone.

### 6.2 Quantitative Component

The quantitative strand is centered on the use of a **Dynamic Bayesian Network (DBN)** to model temporal and structural patterns of engagement. Rather than repeating the full technical description already presented in **Part 2**, this section emphasizes the analytical role of the quantitative component: to identify behavioral regularities, transitions, and engagement trajectories from OULAD clickstream data. The DBN is therefore used to infer probabilistic dependencies among engagement-related indicators over time, supporting the interpretation of engagement as a latent construct rather than as a directly observed variable.

### 6.3 Qualitative Component

The qualitative strand consists of **semi-structured interviews** analyzed through **thematic analysis**. Again, the procedural overview appears in **Part 2**, while this section clarifies the interpretive purpose of the qualitative component. Specifically, the interviews are intended to identify mechanisms that help explain the quantitative patterns, including students’ perceptions of value, effort, workload, routine formation, and invisible forms of study not captured in LMS logs.

### 6.4 Unified Artifact: Latent Engagement Joint Display (LE-JD)

A central contribution of this study is the development of a unifying artifact termed **Latent Engagement Joint Display (LE-JD)**.

The LE-JD extends the earlier idea of Latent Engagement Mapping by explicitly grounding the artifact in the mixed methods literature on **joint displays**. Rather than serving only as a summary table, the LE-JD is conceived as a structured analytical device for representing integration, aligning qualitative and quantitative evidence, and supporting the generation of meta-inferences. This approach follows the view that joint displays are not merely reporting tools, but also frameworks that help researchers organize data, compare findings, and identify integrated interpretations (Guetterman et al., 2015; Guetterman et al., 2021).

The artifact is designed according to several methodological principles emphasized in the literature. First, integration must be explicit rather than implied. Second, the artifact should facilitate the identification of meta-inferences rather than simply place qualitative and quantitative findings side by side. Third, qualitative and quantitative evidence should be represented at a comparable level of aggregation, since direct comparison between raw qualitative quotes and aggregated quantitative statistics can weaken interpretability and the coherence of integration (Guetterman et al., 2021). Finally, the display should improve clarity rather than introduce unnecessary visual or conceptual complexity.

Accordingly, the LE-JD aligns four core elements for each analytical unit:
- Behavioral indicators derived from OULAD
- Latent engagement states inferred from the quantitative model
- Qualitative mechanisms derived from thematic analysis
- Meta-inferences generated through integration

A simplified representation of the artifact is shown below:

| Student | Week | Behavioral Indicators | Latent Engagement | Mechanism | Meta-Inference |
|--------|------|----------------------|------------------|-----------|----------------|
| S1 | W3 | High clicks, low submission | Medium | Cognitive overload | Engagement without effective processing |
| S2 | W5 | No activity, high recency | Low | Low perceived value | Disengagement driven by utility perception |
| S3 | W2–6 | Stable activity, high streak | High | Habit formation | Sustained engagement through routine |

In methodological terms, the LE-JD functions as a **hybrid joint display**. It incorporates features of side-by-side displays by aligning qualitative and quantitative findings, features of statistics-by-themes displays by relating behavioral indicators to qualitative mechanisms, and features of model-based displays by introducing latent engagement as an interpretive layer. This hybrid structure is consistent with the recent evolution of joint displays toward more creative and analytically powerful visual formats (Guetterman et al., 2021).

A key refinement in the LE-JD is the deliberate alignment of aggregation levels across strands. Quantitative findings are represented through aggregated behavioral indicators and inferred latent states, while qualitative findings are represented through themes or mechanisms rather than isolated raw quotes. Quotes may still appear in supporting materials or as illustrative evidence, but the main artifact prioritizes thematic aggregation in order to strengthen comparability and interpretive fit. This design choice is directly supported by recommendations from the methodological review of visual joint displays, which found that integration is stronger when both strands are displayed at a consistent level of aggregation (Guetterman et al., 2021).

The LE-JD is also intended to support visual augmentation. In addition to the tabular display, the artifact may incorporate temporal trajectory plots, state-transition diagrams, or conceptual figures that link mechanisms to behavioral indicators. Such visual extensions are justified by the methodological literature, which argues that visual joint displays can reduce cognitive burden, communicate complex integrated findings more effectively, and support analytic reasoning during the integration process (Guetterman et al., 2021).

Most importantly, the LE-JD is not only a means of presenting results after analysis is complete. It also functions as a tool for conducting integration. In the process of deciding what to include, how to align the strands, and how to interpret convergence, divergence, or expansion, the artifact actively supports the development of mixed methods meta-inferences. In this sense, it is both an analytical framework and a reporting mechanism.

### 6.5 Integration Strategy

The integration of quantitative and qualitative data follows a three-stage process grounded in **merging**, which is one of the most widely used integration strategies in mixed methods joint displays (Fetters et al., 2013; Guetterman et al., 2021).

First, quantitative analysis identifies patterns of engagement, including persistence, transitions, and drop-off points.

Second, qualitative analysis identifies mechanisms that help explain these patterns, such as motivation, habit formation, perceived value of tasks, or cognitive overload.

Finally, both strands are brought together through the LE-JD, which enables the explicit comparison of findings and the construction of meta-inferences. These meta-inferences may reflect confirmation, expansion, or discordance between strands, thereby producing insights that would not emerge from either component alone.

This approach ensures that mixed methods integration is not merely descriptive but analytically grounded, visually explicit, and methodologically justified.

## 7. Expected Contributions

This study contributes to the literature in three key ways.

Theoretically, it reframes student engagement as a latent construct, addressing longstanding challenges in its definition and measurement.

Methodologically, it introduces a structured approach to mixed methods integration, combining probabilistic modeling with qualitative analysis.

Practically, it provides actionable insights for educators seeking to design and evaluate pedagogical interventions in digital learning environments.

⸻

## 8. Limitations

Several limitations must be acknowledged.

First, engagement remains a latent construct and cannot be directly observed. Second, behavioral proxies derived from LMS data are inherently imperfect. Third, qualitative findings may be influenced by sample size and participant selection. Finally, results may be context-dependent, reflecting the characteristics of OULAD rather than generalizable patterns.

⸻

## 9. Significance

By integrating behavioral data with qualitative insights, this study moves beyond traditional proxy-based approaches to engagement. It provides a framework for understanding not only how students behave in digital learning environments, but also why these behaviors emerge.

⸻

## 📚 References (APA Style)

Carmona, C., Castillo, G., & Millán, E. (2008). Designing a dynamic Bayesian network for modeling students’ learning styles. Proceedings of the IEEE International Conference on Advanced Learning Technologies, 346–350.

Johnson, R. B., Onwuegbuzie, A. J., & Turner, L. A. (2007). Toward a definition of mixed methods research. Journal of Mixed Methods Research, 1(2), 112–133.

Kaser, T., Klingler, S., Schwing, A. G., & Gross, M. (2017). Dynamic Bayesian networks for student modeling. IEEE Transactions on Learning Technologies, 10(4), 450–462.

Lacave, C., Molina, A. I., Fernández, M., & Cruz-Lemus, J. A. (2018). Learning analytics to identify dropout factors of computer science studies through Bayesian networks. Behaviour & Information Technology, 37(10–11), 993–1007.

Nguyen, Q., Rienties, B., & Whitelock, D. (2020). A mixed-method study of how instructors design for learning in online and distance education. Journal of Learning Analytics, 7(3), 64–78.

Nomme, K., & Birol, G. (2014). Course redesign: An evidence-based approach. Canadian Journal for the Scholarship of Teaching and Learning, 5, 1–28.

Reichenberg, R. (2018). Dynamic Bayesian networks in educational measurement: Reviewing and advancing the state of the field. Applied Measurement in Education, 31(4), 335–350.

Scutari, M. (2010). Learning Bayesian networks with the bnlearn R package. Journal of Statistical Software, 35(3), 1–22.

Sinatra, G. M., Heddy, B. C., & Lombardi, D. (2015). The challenges of defining and measuring student engagement in science. Educational Psychologist, 50(1), 1–13.

Viberg, O., Hatakka, M., Balter, O., & Mavroudi, A. (2018). The current landscape of learning analytics in higher education. Computers in Human Behavior, 89, 98–110.

Fetters, M. D., Curry, L. A., & Creswell, J. W. (2013). Achieving integration in mixed methods designs: Principles and practices. Health Services Research, 48(6 Pt 2), 2134–2156.

Guetterman, T. C., Fetters, M. D., & Creswell, J. W. (2015). Integrating quantitative and qualitative results in health science mixed methods research through joint displays. Annals of Family Medicine, 13(6), 554–561.

Guetterman, T. C., Fàbregues, S., & Sakakibara, R. (2021). Visuals in joint displays to represent integration in mixed methods research: A methodological review. Methods in Psychology, 5, 100080.

Wong, A., & Chong, S. (2018). Modelling adult learners’ online engagement behaviour: Proxy measures and its application. Journal of Computers in Education, 5(4), 463–479.


⸻

## Appendix A. Proposed Semi-Structured Interview Protocol

### A.1 Purpose of the Interview

This interview protocol is designed to identify mechanisms underlying student engagement patterns observed in LMS behavioral data. The interview is intended to complement the quantitative strand by capturing dimensions of engagement that are not directly visible in OULAD log data, such as perceived value, motivation, study routines, cognitive effort, barriers, and offline engagement.

The protocol is also designed to support the development of the **Latent Engagement Joint Display (LE-JD)** by producing qualitative themes that can be aligned with quantitative behavioral indicators and latent engagement states.

### A.2 Alignment with Research Questions

The interview primarily contributes to:

- **RQ1:** How can student engagement be modeled as a latent construct using behavioral data from OULAD?
- **RQ3:** What mechanisms explain observed engagement behaviors?
- **RQ4:** How can quantitative and qualitative evidence be integrated through a joint display to produce meaningful meta-inferences?

It may also indirectly support:

- **RQ2:** What temporal and structural patterns characterize engagement trajectories?

RQ2 is primarily quantitative, but some interview responses may help interpret temporal patterns identified in the DBN.

### A.3 Core Domains, Interview Questions, and Links to RQs

#### Domain 1. Study Routines and Engagement Habits

This domain examines how students organize their learning over time and whether recurrent LMS activity reflects stable routines, reactive behavior, or irregular study practices.

**Q1. Can you describe how you usually organized your study routine during the course?**  
**Why it matters:** Helps identify whether engagement emerges as structured routine, opportunistic behavior, or inconsistent participation.  
**Addresses:** RQ3, RQ4

**Q2. Did you tend to study at regular times each week, or did your pattern vary depending on deadlines or other demands?**  
**Why it matters:** Helps explain temporal regularity or instability in behavioral traces.  
**Addresses:** RQ2, RQ3, RQ4

**Q3. What usually prompted you to log into the LMS?**  
**Why it matters:** Clarifies triggers of visible engagement, such as deadlines, curiosity, pressure, or habit.  
**Addresses:** RQ3, RQ4

**Q4. Were there moments when using the LMS became part of your normal routine? If so, how did that happen?**  
**Why it matters:** Directly probes habit formation as a mechanism behind persistent engagement.  
**Addresses:** RQ3, RQ4

#### Domain 2. Perceived Value of LMS Activities

This domain investigates how students interpret the usefulness of course resources and whether behavioral engagement reflects meaningful engagement or selective participation.

**Q5. Which LMS activities or resources felt most useful to your learning? Why?**  
**Why it matters:** Identifies perceived value as a mechanism behind engagement.  
**Addresses:** RQ3, RQ4

**Q6. Were there resources you often ignored, delayed, or used only minimally? Why?**  
**Why it matters:** Helps interpret low-activity patterns and selective engagement.  
**Addresses:** RQ3, RQ4

**Q7. What made an activity feel worth your time?**  
**Why it matters:** Clarifies the criteria students use to allocate attention and effort.  
**Addresses:** RQ1, RQ3, RQ4

**Q8. Did optional activities feel different from required ones? In what way?**  
**Why it matters:** Helps explain why some activities may generate clicks without sustained engagement.  
**Addresses:** RQ3, RQ4

#### Domain 3. Engagement Beyond the LMS

This domain is crucial for addressing data silence, especially cases in which LMS inactivity may not equal disengagement.

**Q9. Did you ever study in ways that were not visible in the LMS, such as using downloaded files, notes, textbooks, or external resources?**  
**Why it matters:** Directly identifies invisible or offline engagement.  
**Addresses:** RQ1, RQ3, RQ4

**Q10. Can you describe situations in which you were learning or studying, but your LMS activity would not show much evidence of that?**  
**Why it matters:** Challenges naive interpretations of low LMS activity.  
**Addresses:** RQ1, RQ3, RQ4

**Q11. Did you ever access materials once and then continue studying them offline?**  
**Why it matters:** Helps reinterpret sparse click patterns that may hide sustained study.  
**Addresses:** RQ1, RQ4

#### Domain 4. Barriers, Overload, and Disengagement

This domain explores why behavioral engagement may decline or become fragmented over time.

**Q12. Were there moments when it became harder to engage with the course? What was happening at that time?**  
**Why it matters:** Identifies barriers and moments of disengagement onset.  
**Addresses:** RQ2, RQ3, RQ4

**Q13. Did workload, confusion, fatigue, or competing responsibilities affect how you used the LMS?**  
**Why it matters:** Captures mechanisms such as cognitive overload and external constraints.  
**Addresses:** RQ3, RQ4

**Q14. Were there times when you accessed materials but did not feel meaningfully engaged with them?**  
**Why it matters:** Separates visible interaction from genuine engagement.  
**Addresses:** RQ1, RQ3, RQ4

**Q15. Did you ever postpone or avoid certain activities even when you knew they were important? Why?**  
**Why it matters:** Helps explain recency, inactivity, and drop-off patterns.  
**Addresses:** RQ2, RQ3, RQ4

#### Domain 5. Course Structure, Deadlines, and Feedback

This domain examines how instructional design shapes behavioral traces.

**Q16. How did deadlines influence the way you used the LMS?**  
**Why it matters:** Helps interpret bursts of activity and temporal clustering.  
**Addresses:** RQ2, RQ3, RQ4

**Q17. Did feedback affect how or when you engaged with the course?**  
**Why it matters:** Identifies how instructional response loops shape engagement.  
**Addresses:** RQ3, RQ4

**Q18. How did the structure of the course influence your participation?**  
**Why it matters:** Links engagement patterns to module design rather than only student traits.  
**Addresses:** RQ3, RQ4

**Q19. Were there aspects of the course design that made engagement easier or harder?**  
**Why it matters:** Identifies environmental and pedagogical mechanisms affecting behavior.  
**Addresses:** RQ3, RQ4

#### Domain 6. Interpretation of Behavioral Patterns

This domain is explicitly designed to support integration with the quantitative strand and LE-JD.

**Q20. Sometimes students show high activity in the LMS but submit little work. What do you think might explain that?**  
**Why it matters:** Probes interpretations of high-click, low-output profiles.  
**Addresses:** RQ1, RQ3, RQ4

**Q21. Sometimes students show low activity in the LMS but still perform reasonably well. Does that reflect anything from your experience?**  
**Why it matters:** Probes mechanisms behind low-click, high-performance patterns.  
**Addresses:** RQ1, RQ3, RQ4

**Q22. What do you think a course log can show accurately about engagement, and what do you think it misses?**  
**Why it matters:** Directly addresses the limits of LMS-derived proxies.  
**Addresses:** RQ1, RQ4

**Q23. What does “being engaged” in a course mean to you?**  
**Why it matters:** Helps compare institutional or analytic definitions with student meanings.  
**Addresses:** RQ1, RQ3, RQ4

#### Closing Question

**Q24. Is there anything important about your engagement with the course that LMS data would not show?**  
**Why it matters:** Captures overlooked dimensions and strengthens the study’s treatment of data silence.  
**Addresses:** RQ1, RQ3, RQ4

### A.4 Suggested Mapping Summary Table

| Domain | Main focus | Main RQs |
|---|---|---|
| Study routines and habits | Temporal organization of engagement | RQ2, RQ3, RQ4 |
| Perceived value of activities | Why students engage selectively | RQ3, RQ4 |
| Engagement beyond the LMS | Invisible or offline engagement | RQ1, RQ3, RQ4 |
| Barriers and disengagement | Why engagement declines or fragments | RQ2, RQ3, RQ4 |
| Course structure and feedback | Role of pedagogy in shaping behavior | RQ3, RQ4 |
| Interpretation of behavioral patterns | Direct bridge to LE-JD integration | RQ1, RQ3, RQ4 |

### A.5 Note on Conceptual Alignment

A possible future refinement of the proposal would be to adjust **RQ1** so that it explicitly reflects the integrated logic of the study. At present, RQ1 emphasizes modeling engagement as a latent construct using behavioral data from OULAD. A more fully aligned formulation might state: **How can student engagement be modeled as a latent construct through the integration of behavioral data from OULAD and qualitative accounts of student experience?**

This refinement is not required for the present proposal, but it would make the relationship between the qualitative strand and the overall conceptualization of engagement even more explicit.