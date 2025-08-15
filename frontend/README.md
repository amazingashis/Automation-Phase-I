# AI Schema Inference Workflow - Frontend

A beautiful web interface for the AI-powered schema inference and Databricks import script generation workflow.

## Features

- 🎨 **Modern UI/UX**: Beautiful gradient design with smooth animations
- 📁 **Drag & Drop Upload**: Easy CSV file upload with drag-and-drop support
- 🤖 **AI-Powered**: Integrates with your existing LLM-based header and schema inference
- 💬 **Real-time Feedback**: Live updates and feedback panel showing workflow progress
- ✏️ **Interactive Editing**: Edit headers and schema types directly in the UI
- 🚀 **Script Generation**: Generate and download Databricks PySpark import scripts
- 📱 **Responsive**: Works on desktop, tablet, and mobile devices

## Files Structure

```
frontend/
├── index.html          # Main HTML structure
├── styles.css          # Beautiful CSS styling with animations
├── script.js           # JavaScript workflow logic
├── app.py             # Flask backend API server
└── README.md          # This file
```

## How to Run

### Option 1: Standalone Frontend (Simulation Mode)
```bash
# Simply open index.html in your browser
# This runs with simulated backend responses
```

### Option 2: Full Integration with Backend
```bash
# Install Flask and dependencies
pip install flask flask-cors

# Run the Flask server
cd frontend
python app.py

# Open browser to: http://localhost:5000
```

## Workflow Steps

1. **📁 Upload CSV**: Drag and drop or click to select your CSV file
2. **📊 Describe Data**: Enter a description of your data type (e.g., "healthcare enrollment")
3. **🏷️ Review Headers**: AI infers headers - approve, edit, or reject
4. **🗂️ Review Schema**: AI infers data types - approve or modify
5. **🚀 Get Script**: Download the generated Databricks PySpark import script

## Features in Detail

### Real-time Feedback Panel
- Shows all workflow steps and progress
- Color-coded messages (info, success, warning, error)
- Timestamps for each action
- Scrollable history of all activities

### Interactive Header Management
- View all inferred headers
- Edit individual headers with one click
- Bulk edit all headers at once
- Validation for proper naming conventions

### Schema Editing
- Review inferred data types
- Modify types directly in the UI
- View full JSON schema
- Validation for supported Spark data types

### Script Download
- Generates complete PySpark script
- Includes proper schema definition
- Delta table creation (optional)
- Download as .py file

## Integration with Your Backend

The frontend is designed to work with your existing Python agents:

- `agent_header_check.py` - Header detection and inference
- `header_infer.py` - LLM-powered header generation
- `infer_schema.py` - Schema inference from CSV data
- `import_schema_generator.py` - PySpark script generation

## API Endpoints (Flask Backend)

- `POST /api/upload` - Upload CSV file
- `POST /api/analyze-headers` - Analyze headers using AI
- `POST /api/generate-script` - Generate import script
- `GET /api/health` - Health check

## Customization

### Styling
Edit `styles.css` to customize:
- Color schemes and gradients
- Animation speeds and effects
- Layout and spacing
- Responsive breakpoints

### Workflow Logic
Edit `script.js` to customize:
- Validation rules
- UI interactions
- API integration
- Data processing

### Backend Integration
Edit `app.py` to:
- Add new API endpoints
- Modify LLM integration
- Add authentication
- Enhance error handling

## Browser Compatibility

- ✅ Chrome/Chromium (recommended)
- ✅ Firefox
- ✅ Safari
- ✅ Edge

## Future Enhancements

- 🔐 User authentication and sessions
- 💾 Save/load workflow states
- 📊 Data preview and statistics
- 🔄 Batch processing multiple files
- 📧 Email notifications
- 🔌 API key management for LLM services

Enjoy your beautiful AI Schema Inference Workflow! 🚀
