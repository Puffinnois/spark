import matplotlib.pyplot as plt

# Résultats des temps d'exécution
methods = ["Sans UDF", "UDF Python", "UDF Scala"]
execution_times = [6.2000, 5.4187, 0.6247]  # Remplacez par vos valeurs mesurées

# Création du graphique
plt.bar(methods, execution_times, color=['red', 'blue', 'green'])
plt.xlabel("Méthode")
plt.ylabel("Temps d'exécution (secondes)")
plt.title("Comparaison des performances des méthodes UDF")
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Enregistrer le graphique
plt.savefig("src/fr/hymaia/exo4/performance_comparison.png")
print("Graphique enregistré sous : src/fr/hymaia/exo4/performance_comparison.png")
