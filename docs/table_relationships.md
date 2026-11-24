Table Relationships
Patient │ ├── Encounter │ ├── Observation │ └── Condition

Relationships:

One Patient can have many Encounters
One Patient can have many Observations
One Patient can have many Conditions


Patient (1) ---- (*) Encounter

Patient (1) ---- (*) Observation

Patient (1) ---- (*) Condition