from flask import Flask, request, jsonify
import base64

app = Flask(__name__)

@app.route('/decode-http-data', methods=['POST'])
def decode_http_data():
    try:
        # Extract the data from the incoming request
        encoded_data = request.json.get('data')
        
        # Decoding logic (base64 as an example)
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        
        # Return the decoded data
        return jsonify({
            'status': 'success',
            'decoded_data': decoded_data
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 400

if __name__ == '__main__':
    app.run(debug=True)
