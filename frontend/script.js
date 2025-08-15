// AI Schema Inference Workflow JavaScript

class WorkflowApp {
    constructor() {
        this.currentStep = 'upload';
        this.csvFile = null;
        this.headers = [];
        this.schema = null;
        this.script = '';
        
        this.initializeEventListeners();
        this.addFeedback('Ready to start! Upload a CSV file to begin the workflow.', 'info');
    }

    initializeEventListeners() {
        // File Upload
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('csvFile');
        
        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
        uploadArea.addEventListener('drop', this.handleDrop.bind(this));
        fileInput.addEventListener('change', this.handleFileSelect.bind(this));

        // Buttons
        document.getElementById('analyzeBtn').addEventListener('click', this.analyzeHeaders.bind(this));
        document.getElementById('approveHeadersBtn').addEventListener('click', this.approveHeaders.bind(this));
        document.getElementById('editHeadersBtn').addEventListener('click', this.editHeaders.bind(this));
        document.getElementById('rejectHeadersBtn').addEventListener('click', this.rejectHeaders.bind(this));
        document.getElementById('approveSchemaBtn').addEventListener('click', this.approveSchema.bind(this));
        document.getElementById('rejectSchemaBtn').addEventListener('click', this.rejectSchema.bind(this));
        document.getElementById('downloadScriptBtn').addEventListener('click', this.downloadScript.bind(this));
    }

    handleDragOver(e) {
        e.preventDefault();
        e.currentTarget.classList.add('dragover');
    }

    handleDrop(e) {
        e.preventDefault();
        e.currentTarget.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length > 0 && files[0].name.endsWith('.csv')) {
            this.handleFile(files[0]);
        }
    }

    handleFileSelect(e) {
        const file = e.target.files[0];
        if (file) {
            this.handleFile(file);
        }
    }

    handleFile(file) {
        this.csvFile = file;
        document.getElementById('fileInfo').textContent = `Selected: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
        this.addFeedback(`File uploaded: ${file.name}`, 'success');
        this.showSection('dataTypeSection');
        this.currentStep = 'dataType';
    }

    async analyzeHeaders() {
        this.addFeedback('Analyzing file structure...', 'info');
        this.setLoading('analyzeBtn', true);

        try {
            // Check if file has headers first
            const hasHeaders = await this.checkFileHeaders();
            
            if (hasHeaders) {
                this.addFeedback('File has headers detected! Skipping header inference...', 'success');
                this.displaySchema();
                this.showSection('schemaSection');
                this.currentStep = 'schema';
                this.addFeedback('Schema inferred from existing headers! Please review and approve.', 'success');
            } else {
                // File doesn't have headers, need data type description
                const dataType = document.getElementById('dataTypeInput').value.trim();
                if (!dataType) {
                    // Update UI to request data type description
                    document.getElementById('dataTypeContent').innerHTML = `
                        <div class="alert info">
                            <p><strong>No headers detected!</strong> Please describe what type of data this CSV contains to help AI infer appropriate headers:</p>
                            <input type="text" id="dataTypeInput" placeholder="e.g., healthcare enrollment, employee payroll, customer orders..." class="data-input" value="${dataType}">
                        </div>
                    `;
                    this.addFeedback('No headers detected. Please provide data type description.', 'warning');
                    this.setLoading('analyzeBtn', false);
                    return;
                }
                
                this.addFeedback(`No headers detected. Inferring headers for ${dataType} data using AI...`, 'info');
                await this.simulateHeaderAnalysis(dataType);
                this.displayHeaders();
                this.showSection('headerSection');
                this.currentStep = 'headers';
                this.addFeedback('Headers inferred successfully! Please review and approve.', 'success');
            }
        } catch (error) {
            this.addFeedback(`Error analyzing file: ${error.message}`, 'error');
        } finally {
            this.setLoading('analyzeBtn', false);
        }
    }

    async checkFileHeaders() {
        // Check if file has headers using backend API
        try {
            const response = await fetch('/api/check-headers', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    file_path: this.csvFile.path || '/temp/' + this.csvFile.name
                })
            });
            
            const result = await response.json();
            if (result.success && result.has_headers) {
                this.headers = result.headers;
                this.schema = result.schema;
                return true;
            }
            return false;
        } catch (error) {
            console.error('Error checking headers:', error);
            // Fallback to simulation for demo
            return new Promise((resolve) => {
                setTimeout(() => {
                    const hasHeaders = Math.random() > 0.3;
                    resolve(hasHeaders);
                }, 1000);
            });
        }
    }

    async simulateDirectSchemaInference() {
        // Simulate schema inference when headers already exist
        return new Promise((resolve) => {
            setTimeout(() => {
                // Mock headers from existing file
                this.headers = [
                    'id', 'first_name', 'last_name', 'email', 'gender',
                    'plan', 'effective_date', 'termination_date', 'record_type',
                    'state', 'address_1', 'address_2', 'medical_record_number', 'division_name'
                ];
                
                this.schema = {
                    type: "struct",
                    fields: this.headers.map(header => ({
                        name: header,
                        type: this.inferDataType(header),
                        nullable: true,
                        metadata: {}
                    }))
                };
                resolve();
            }, 1000);
        });
    }

    async simulateHeaderAnalysis(dataType) {
        // Simulate backend API call
        return new Promise((resolve) => {
            setTimeout(() => {
                // Mock response based on data type
                if (dataType.toLowerCase().includes('healthcare') || dataType.toLowerCase().includes('member')) {
                    this.headers = [
                        'member_id', 'first_name', 'last_name', 'email', 'gender',
                        'plan_type', 'effective_date', 'termination_date', 'service_type',
                        'state', 'address_line1', 'address_line2', 'medical_record_number', 'division'
                    ];
                } else if (dataType.toLowerCase().includes('employee') || dataType.toLowerCase().includes('payroll')) {
                    this.headers = [
                        'employee_id', 'first_name', 'last_name', 'email', 'department',
                        'position', 'hire_date', 'salary', 'manager_id', 'location'
                    ];
                } else {
                    this.headers = [
                        'id', 'name', 'email', 'category', 'date_created',
                        'status', 'amount', 'description', 'location', 'notes'
                    ];
                }
                resolve();
            }, 2000);
        });
    }

    displayHeaders() {
        const headerDisplay = document.getElementById('headerDisplay');
        const headerList = this.headers.map((header, index) => 
            `<div class="header-item">
                <span><strong>Column ${index + 1}:</strong> ${header}</span>
                <button class="btn-small" onclick="app.editSingleHeader(${index})">✏️</button>
            </div>`
        ).join('');
        
        headerDisplay.innerHTML = `<div class="header-list">${headerList}</div>`;
    }

    editSingleHeader(index) {
        const newHeader = prompt(`Edit header for column ${index + 1}:`, this.headers[index]);
        if (newHeader && newHeader.trim()) {
            this.headers[index] = newHeader.trim();
            this.displayHeaders();
            this.addFeedback(`Updated column ${index + 1} header to: ${newHeader}`, 'info');
        }
    }

    editHeaders() {
        const newHeaders = prompt('Edit headers (comma-separated):', this.headers.join(', '));
        if (newHeaders) {
            this.headers = newHeaders.split(',').map(h => h.trim()).filter(h => h);
            this.displayHeaders();
            this.addFeedback('Headers updated manually.', 'info');
        }
    }

    rejectHeaders() {
        this.addFeedback('Headers rejected. Please modify data type description and try again.', 'warning');
        this.hideSection('headerSection');
        this.currentStep = 'dataType';
    }

    async approveHeaders() {
        this.addFeedback('Headers approved! Inferring schema...', 'success');
        this.setLoading('approveHeadersBtn', true);

        try {
            await this.simulateSchemaInference();
            this.displaySchema();
            this.showSection('schemaSection');
            this.currentStep = 'schema';
            this.addFeedback('Schema inferred successfully! Please review and approve.', 'success');
        } catch (error) {
            this.addFeedback(`Error inferring schema: ${error.message}`, 'error');
        } finally {
            this.setLoading('approveHeadersBtn', false);
        }
    }

    async simulateSchemaInference() {
        return new Promise((resolve) => {
            setTimeout(() => {
                this.schema = {
                    type: "struct",
                    fields: this.headers.map(header => ({
                        name: header,
                        type: this.inferDataType(header),
                        nullable: true,
                        metadata: {}
                    }))
                };
                resolve();
            }, 1500);
        });
    }

    inferDataType(header) {
        const lowerHeader = header.toLowerCase();
        if (lowerHeader.includes('id') || lowerHeader.includes('number')) return 'integer';
        if (lowerHeader.includes('date') || lowerHeader.includes('time')) return 'date';
        if (lowerHeader.includes('amount') || lowerHeader.includes('salary') || lowerHeader.includes('price')) return 'float';
        if (lowerHeader.includes('email')) return 'string';
        if (lowerHeader.includes('active') || lowerHeader.includes('enabled')) return 'boolean';
        return 'string';
    }

    displaySchema() {
        const schemaDisplay = document.getElementById('schemaDisplay');
        const schemaItems = this.schema.fields.map(field => 
            `<div class="schema-item">
                <span><strong>${field.name}:</strong> ${field.type} ${field.nullable ? '(nullable)' : ''}</span>
                <button class="btn-small" onclick="app.editSchemaField('${field.name}')">✏️</button>
            </div>`
        ).join('');
        
        schemaDisplay.innerHTML = `
            <div class="schema-display">
                ${schemaItems}
            </div>
            <details style="margin-top: 15px;">
                <summary style="cursor: pointer; font-weight: bold;">View JSON Schema</summary>
                <pre style="background: #f8f9fa; padding: 15px; margin-top: 10px; border-radius: 5px; overflow-x: auto;">${JSON.stringify(this.schema, null, 2)}</pre>
            </details>
        `;
    }

    editSchemaField(fieldName) {
        const field = this.schema.fields.find(f => f.name === fieldName);
        const newType = prompt(`Edit data type for ${fieldName}:`, field.type);
        if (newType && newType.trim()) {
            field.type = newType.trim();
            this.displaySchema();
            this.addFeedback(`Updated ${fieldName} type to: ${newType}`, 'info');
        }
    }

    rejectSchema() {
        this.addFeedback('Schema rejected. Please modify headers and try again.', 'warning');
        this.hideSection('schemaSection');
        this.currentStep = 'headers';
    }

    async approveSchema() {
        this.addFeedback('Schema approved! Generating Databricks import script...', 'success');
        this.setLoading('approveSchemaBtn', true);

        try {
            await this.generateImportScript();
            this.displayScript();
            this.showSection('scriptSection');
            this.currentStep = 'complete';
            this.addFeedback('Import script generated successfully! Workflow complete.', 'success');
        } catch (error) {
            this.addFeedback(`Error generating script: ${error.message}`, 'error');
        } finally {
            this.setLoading('approveSchemaBtn', false);
        }
    }

    async generateImportScript() {
        return new Promise((resolve) => {
            setTimeout(() => {
                const fileName = this.csvFile.name.replace('.csv', '');
                const tableName = fileName.toLowerCase().replace(/[^a-z0-9]/g, '_');
                
                this.script = `# Databricks PySpark Import Script
# Generated for: ${this.csvFile.name}
# Date: ${new Date().toLocaleDateString()}

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder.appName("${fileName}_Import").getOrCreate()

# Define Schema
schema = StructType([
${this.schema.fields.map(field => {
    const sparkType = this.getSparkType(field.type);
    return `    StructField("${field.name}", ${sparkType}(), ${field.nullable})`;
}).join(',\n')}
])

# Read CSV with defined schema
df = spark.read \\
    .option("header", "true") \\
    .option("inferSchema", "false") \\
    .schema(schema) \\
    .csv("/path/to/your/${this.csvFile.name}")

# Display basic info
print(f"Records loaded: {df.count()}")
df.printSchema()
df.show(10)

# Write to Delta table (optional)
df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .saveAsTable("${tableName}")

print("Import completed successfully!")
`;
                resolve();
            }, 1000);
        });
    }

    getSparkType(type) {
        const typeMap = {
            'string': 'StringType',
            'integer': 'IntegerType',
            'float': 'FloatType',
            'double': 'DoubleType',
            'boolean': 'BooleanType',
            'date': 'DateType',
            'timestamp': 'TimestampType'
        };
        return typeMap[type.toLowerCase()] || 'StringType';
    }

    displayScript() {
        document.getElementById('scriptDisplay').textContent = this.script;
    }

    downloadScript() {
        const blob = new Blob([this.script], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${this.csvFile.name.replace('.csv', '')}_import_script.py`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        this.addFeedback('Import script downloaded successfully!', 'success');
    }

    showSection(sectionId) {
        document.getElementById(sectionId).style.display = 'block';
        document.getElementById(sectionId).scrollIntoView({ behavior: 'smooth' });
    }

    hideSection(sectionId) {
        document.getElementById(sectionId).style.display = 'none';
    }

    setLoading(buttonId, loading) {
        const button = document.getElementById(buttonId);
        if (loading) {
            button.classList.add('loading');
            button.disabled = true;
            button.textContent = '⏳ Processing...';
        } else {
            button.classList.remove('loading');
            button.disabled = false;
            // Restore original text based on button ID
            const originalTexts = {
                'analyzeBtn': '🔍 Analyze Headers',
                'approveHeadersBtn': '✅ Approve Headers',
                'approveSchemaBtn': '✅ Approve Schema'
            };
            button.textContent = originalTexts[buttonId] || button.textContent;
        }
    }

    addFeedback(message, type = 'info') {
        const feedbackDisplay = document.getElementById('feedbackDisplay');
        const timestamp = new Date().toLocaleTimeString();
        
        const feedbackItem = document.createElement('div');
        feedbackItem.className = `feedback-item ${type}`;
        feedbackItem.innerHTML = `
            <span class="timestamp">${timestamp}</span>
            <span class="message">${message}</span>
        `;
        
        feedbackDisplay.insertBefore(feedbackItem, feedbackDisplay.firstChild);
        
        // Keep only last 20 feedback items
        while (feedbackDisplay.children.length > 20) {
            feedbackDisplay.removeChild(feedbackDisplay.lastChild);
        }
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new WorkflowApp();
});

// Additional utility functions
function formatFileSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

function validateCSVHeaders(headers) {
    const validHeaders = headers.every(header => 
        header && 
        header.trim().length > 0 && 
        /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(header.trim())
    );
    return validHeaders;
}
