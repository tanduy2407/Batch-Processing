from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/get-user/<user_id>')
def get_user(user_id):
    data = {
        'user_id': user_id,
        'name': 'Duy',
        'email': 'duy@email.com'
    }
    extra = request.args.get('extra')
    if extra:
        data['extra'] = extra

    return jsonify(data), 200

if __name__ == '__main__':
    app.run(debug=True)