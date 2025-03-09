import streamlit as st

def main():
    st.title("Simple Streamlit App")
    st.write("Welcome to your first Streamlit app!")

    name = st.text_input("Enter your name:")
    if name:
        st.write(f"Hello, {name}!")

if __name__ == "__main__":
    main()
